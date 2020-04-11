package ds.hdfs;

import proto.ProtoHDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DataNode extends UnicastRemoteObject implements DataNodeInterface {
    // This data structure allows thread safe access to the blocks of this specific data node
    protected ConcurrentHashMap<String, Boolean> requestsFulfilled;
    protected ConcurrentHashMap<String, ProtoHDFS.BlockMeta> blockMetas;

    protected String dataId;
    protected String dataIp;
    protected int dataPort;

    private static DataNode dataNodeInstance = null;

    private DataNode(String dataId, String dataIp) throws RemoteException {
        super();
        this.dataId = dataId;
        this.dataIp = dataIp;
        this.dataPort = 1099;
        this.requestsFulfilled = new ConcurrentHashMap<>();
        this.blockMetas = new ConcurrentHashMap<>();
    }

    private DataNode(String dataId, String dataIp, int port) throws RemoteException {
        super(port);
        this.dataId = dataId;
        this.dataIp = dataIp;
        this.dataPort = port;
        this.requestsFulfilled = new ConcurrentHashMap<>();
        this.blockMetas = new ConcurrentHashMap<>();
    }

    // Overloaded singleton pattern
    public static DataNode getDataNodeInstance(String nodeId, String nodeIp) throws RemoteException{
        if(dataNodeInstance == null){
            dataNodeInstance = new DataNode(nodeId, nodeIp);
        }
        return dataNodeInstance;
    }

    public static DataNode getDataNodeInstance(String nodeId, String nodeIp, int port) throws RemoteException{
        if(dataNodeInstance == null){
            dataNodeInstance = new DataNode(nodeId, nodeIp, port);
        }
        return dataNodeInstance;
    }

    @Override
    public byte[] readBlock(byte[] inp) throws IOException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        // If request has never been made before or hasn't been fulfilled, read the block
        // If the request has been fulfilled already, just do the operation again since it's just a read operation
        if(!this.requestsFulfilled.containsKey(requestId) || !this.requestsFulfilled.get(requestId)){
            this.requestsFulfilled.putIfAbsent(requestId, false);
        }

        // A request sent to readBlocks should only contain a block list consisting of a single block
        List<ProtoHDFS.Block> requestBlockList = request.getBlockList();
        LinkedList<ProtoHDFS.Block> blockList = new LinkedList<>(requestBlockList);

        ProtoHDFS.Block block = blockList.pop();
        ProtoHDFS.BlockMeta blockMeta = block.getBlockMeta();

        String fileName = blockMeta.getFileName();
        int blockNumber = blockMeta.getBlockNumber();
        int repNumber = blockMeta.getRepNumber();
        String blockName = fileName + "_" + blockNumber + "_" + repNumber;
        String blockPath = "./" + this.dataId + "/" + blockName;

        if(this.blockMetas.containsKey(blockPath)){
            byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
            String blockContents = new String(blockBytes);

            ProtoHDFS.Block.Builder blockBuilder = ProtoHDFS.Block.newBuilder();
            blockBuilder.setBlockMeta(this.blockMetas.get(blockName));
            blockBuilder.setBlockContents(blockContents);
            ProtoHDFS.Block responseBlock = blockBuilder.build();
            blockBuilder.clear();

            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
            responseBuilder.setBlock(responseBlock);
            responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s read success",
                    blockNumber, repNumber, fileName));
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();

            this.requestsFulfilled.replace(requestId, true);
            return response.toByteArray();
        }else{
            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
            responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s does not exist",
                    blockNumber, repNumber, fileName));
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();

            this.requestsFulfilled.replace(requestId, true);
            return response.toByteArray();
        }
    }

    @Override
    public byte[] writeBlock(byte[] inp) throws IOException, NotBoundException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        ProtoHDFS.Request.RequestType requestType = request.getRequestType();
        // If request has never been made before or hasn't been fulfilled, read the block
        // If the request has been fulfilled already, just do the operation again since it's just a read operation
        if(!this.requestsFulfilled.containsKey(requestId) || !this.requestsFulfilled.get(requestId)){
            this.requestsFulfilled.putIfAbsent(requestId, false);
        }

        // Make the replication factor configurable later
        Properties hdfsProp = new Properties();
        File hdfsPropFile = new File("hdfs.properties");
        FileInputStream hdfsPropInputStream = new FileInputStream(hdfsPropFile);
        hdfsProp.load(hdfsPropInputStream);

        int repFactor = Integer.parseInt(hdfsProp.getProperty("rep_factor", "3"));
        List<ProtoHDFS.Block> requestBlockList = request.getBlockList();
        LinkedList<ProtoHDFS.Block> blockList = new LinkedList<>(requestBlockList);

        System.out.println(blockList.size());

        if(blockList.size() == 0){
            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
            responseBuilder.setErrorMessage("All replications successful");
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();
            return response.toByteArray();
        }

        ProtoHDFS.Block block = blockList.pop();
        ProtoHDFS.BlockMeta blockMeta = block.getBlockMeta();
        String blockContents = block.getBlockContents();

        String fileName = blockMeta.getFileName();
        int blockNumber = blockMeta.getBlockNumber();
        int repNumber = blockMeta.getRepNumber();

        if(blockList.size() > 0){
            // Send a 'request' object to the next data node to replicate block on another data node
            // if the number of replicas is not enough
            ProtoHDFS.Request.Builder requestBuilder = ProtoHDFS.Request.newBuilder();
            String replicateRequestId = UUID.randomUUID().toString();
            this.requestsFulfilled.putIfAbsent(replicateRequestId, false);

            requestBuilder.setRequestId(replicateRequestId);
            requestBuilder.setRequestType(requestType);
            requestBuilder.addAllBlock(blockList);
            ProtoHDFS.Request replicateRequest = requestBuilder.buildPartial();
            requestBuilder.clear();

            ProtoHDFS.Block nextBlock = blockList.peek();
            assert nextBlock != null;
            ProtoHDFS.BlockMeta nextBlockMeta = nextBlock.getBlockMeta();

            String dataNodeId = nextBlockMeta.getDataId();
            String dataNodeIp = nextBlockMeta.getDataIp();
            int dataPort = nextBlockMeta.getPort();
            DataNodeInterface dataStub = getDNStub(dataNodeId, dataNodeIp, dataPort);

            byte[] replicateResponseBytes = null;
            while(replicateResponseBytes == null){
                replicateResponseBytes = dataStub.writeBlock(replicateRequest.toByteArray());
            }

            this.requestsFulfilled.replace(replicateRequestId, true);
            ProtoHDFS.Response replicateResponse = ProtoHDFS.Response.parseFrom(replicateResponseBytes);
            ProtoHDFS.Response.ResponseType repStatus = replicateResponse.getResponseType();
            if(repStatus == ProtoHDFS.Response.ResponseType.FAILURE){
                ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
                responseBuilder.setResponseId(requestId);
                responseBuilder.setErrorMessage("Failed to replicate block");
                ProtoHDFS.Response response = responseBuilder.buildPartial();
                responseBuilder.clear();
                return response.toByteArray();
            }
        }

        String blockName = fileName + "_" + blockNumber + "_" + repNumber;
        File file = new File("./" + this.dataId + "/" + blockName);
        FileOutputStream fileOutputStream;
        if(file.exists() || file.createNewFile()){
            fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(blockContents.getBytes());
            fileOutputStream.flush();
            fileOutputStream.close();
            blockMeta = blockMeta.toBuilder().setInitialized(true).build();
            this.blockMetas.putIfAbsent(blockName, blockMeta);
        }else{
            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setErrorMessage("Failed to write block to data node");
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();

            this.requestsFulfilled.replace(requestId, true);
            return response.toByteArray();
        }

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
        responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s write success",
                blockNumber, repNumber, fileName));
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();

        this.requestsFulfilled.replace(requestId, true);
        return response.toByteArray();
    }

    public DataNodeInterface getDNStub(String dataId, String dataIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(dataIp, port);
                DataNodeInterface dataStub = (DataNodeInterface)registry.lookup(dataId);
                // System.out.println("Data Node Found!");
                return dataStub;
            }catch(Exception e){
                // System.out.println("Searching for data node");
            }
        }
    }

    public NameNodeInterface getNNStub(String nameId, String nameIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(nameIp, port);
                NameNodeInterface nameStub = (NameNodeInterface)registry.lookup(nameId);
                // System.out.println("Name Node Found!");
                return nameStub;
            }catch(Exception e){
                // System.out.println("Searching for name node");
            }
        }
    }

    public static void main(String[] args) {
        // Upon startup of the data node

        while (true) {
            Properties prop = new Properties();
            try {
                // Bind data node to registry
                String dataNodeId = InetAddress.getLocalHost().getHostName();
                String dataNodeIp = InetAddress.getLocalHost().getHostAddress();
                int dataPort = (args.length == 0) ? 1099 : Integer.parseInt(args[0]);

                if(dataNodeInstance == null){
                    Registry serverRegistry = LocateRegistry.createRegistry(dataPort);
                    DataNode newDataNode = (args.length == 0) ? getDataNodeInstance(dataNodeId, dataNodeIp) :
                            getDataNodeInstance(dataNodeId, dataNodeIp, dataPort);
                    serverRegistry.bind(dataNodeId, newDataNode);
                }

                // Now create directory to store all the blocks on this data node
                File dataNodeDir = new File("./" + dataNodeId);
                if (!dataNodeDir.exists()) {
                    boolean dirCreated = dataNodeDir.mkdir();
                    if (!dirCreated) {
                        // If the data node directory has not been created, throw exception and stop the thing
                        throw new Exception("Failed to create data node directory");
                    }
                }

            /*
            if(dataNodeDir.exists()){
                // If data node directory already exists, that means the data node has failed before and is restarting
                File[] dataNodeFiles = dataNodeDir.listFiles();
                assert dataNodeFiles != null;
                ProtoHDFS.BlockMeta.Builder blockMetaBuilder = ProtoHDFS.BlockMeta.newBuilder();
                for(File f : dataNodeFiles){
                    if(f.isFile()){
                        String blockName = f.getName();
                        int secondUnderIndex = -1;
                        int firstUnderIndex = -1;
                        for(int i = blockName.length() - 1; i >= 0; i--){
                            if(blockName.charAt(i) == '_' && secondUnderIndex == -1){
                                secondUnderIndex = i;
                            }else if(blockName.charAt(i) == '_'){
                                firstUnderIndex = i;
                                break;
                            }
                        }

                        String fileName = blockName.substring(0, firstUnderIndex);
                        int blockNumber = Integer.parseInt(blockName.substring(firstUnderIndex + 1, secondUnderIndex));
                        int repNumber = Integer.parseInt(blockName.substring(secondUnderIndex + 1));

                        blockMetaBuilder.setFileName(fileName);
                        blockMetaBuilder.setBlockNumber(blockNumber);
                        blockMetaBuilder.setRepNumber(repNumber);
                        blockMetaBuilder.setDataId(dataNodeId);
                        blockMetaBuilder.setDataIp(dataNodeIp);
                        blockMetaBuilder.setPort(dataPort);
                        blockMetaBuilder.setInitialized(true);
                        ProtoHDFS.BlockMeta blockMeta = blockMetaBuilder.build();
                        blockMetaBuilder.clear();

                        newDataNode.blockMetas.putIfAbsent(blockName, blockMeta);
                    }
                }
            }else{
                // If data node directory does not exist, then create the directory
                boolean dirCreated = dataNodeDir.mkdir();
                if(!dirCreated){
                    // If the data node directory has not been created, throw exception and stop the thing
                    throw new Exception("Failed to create data node directory");
                }
            }

             */

                // System.out.println("Data Node " + dataNodeId + " is running on host " + dataNodeIp + " port " + dataPort);

                // Gets the file handle to the namenode.properties file
                File propFile = new File("namenode.properties");
                FileInputStream fileInputStream = new FileInputStream(propFile);
                prop.load(fileInputStream);

                String nameNodeId = prop.getProperty("server_name");
                String nameNodeIp = prop.getProperty("server_ip");
                int namePort = Integer.parseInt(prop.getProperty("server_port"));

                // Connect data node to the name node and start sending heartbeats and block reports
                ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
                scheduledExecutorService.scheduleAtFixedRate(
                        new SendHeartbeatBlockReportTask(nameNodeId, nameNodeIp, namePort, dataNodeInstance),
                        0, 2, TimeUnit.SECONDS);

                Thread.sleep(2000);
            } catch (Exception e) {
                System.out.println("An error has occurred: " + e.getMessage());
                e.printStackTrace();
                return;
            }
        }
    }
}

