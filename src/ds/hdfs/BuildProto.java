package ds.hdfs;

import proto.ProtoHDFS;

import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;

public class BuildProto {

    public static ProtoHDFS.BlockMeta buildBlockMeta(ProtoHDFS.BlockMeta.Builder blockMetaBuilder, String fileName,
                                                     int blockNumber, int repNumber, String dataId, String dataIp,
                                                     int port, boolean initialized){
        blockMetaBuilder.setFileName(fileName);
        blockMetaBuilder.setBlockNumber(blockNumber);
        blockMetaBuilder.setRepNumber(repNumber);
        blockMetaBuilder.setDataId(dataId);
        blockMetaBuilder.setDataIp(dataIp);
        blockMetaBuilder.setPort(port);
        blockMetaBuilder.setInitialized(initialized);
        ProtoHDFS.BlockMeta blockMeta = blockMetaBuilder.build();
        blockMetaBuilder.clear();
        return blockMeta;
    }

    public static ProtoHDFS.Block buildBlock(ProtoHDFS.Block.Builder blockBuilder, ProtoHDFS.BlockMeta blockMeta,
                                                 String blockContents){
        blockBuilder.setBlockMeta(blockMeta);
        if(blockContents != null) blockBuilder.setBlockContents(blockContents);
        ProtoHDFS.Block block = blockBuilder.buildPartial();
        blockBuilder.clear();
        return block;
    }

    public static ProtoHDFS.Pipeline buildPipeline(ProtoHDFS.Pipeline.Builder pipelineBuilder, int pipelineNumber,
                                                   List<ProtoHDFS.Block> blockList){
        pipelineBuilder.setPipelineNumber(pipelineNumber);
        if(blockList != null) pipelineBuilder.addAllBlocks(blockList);
        ProtoHDFS.Pipeline pipeline = pipelineBuilder.build();
        pipelineBuilder.clear();
        return pipeline;
    }

    public static ProtoHDFS.FileHandle buildFileHandle(ProtoHDFS.FileHandle.Builder fileHandleBuilder, String fileName,
                                                       int fileSize, List<ProtoHDFS.Pipeline> pipelines){
        fileHandleBuilder.setFileName(fileName);
        fileHandleBuilder.setFileSize(fileSize);
        if(pipelines != null) fileHandleBuilder.addAllPipelines(pipelines);
        ProtoHDFS.FileHandle fileHandle = fileHandleBuilder.buildPartial();
        fileHandleBuilder.clear();
        return fileHandle;
    }

    public static ProtoHDFS.Request buildRequest(ProtoHDFS.Request.Builder requestBuilder, String requestId,
                                                 ProtoHDFS.Request.RequestType requestType,
                                                 ProtoHDFS.FileHandle fileHandle, List<ProtoHDFS.Block> blocks){
        requestBuilder.setRequestId(requestId);
        requestBuilder.setRequestType(requestType);
        if(fileHandle != null) requestBuilder.setFileHandle(fileHandle);
        if(blocks != null) requestBuilder.addAllBlock(blocks);
        ProtoHDFS.Request request = requestBuilder.buildPartial();
        requestBuilder.clear();
        return request;
    }

    public static ProtoHDFS.Response buildResponse(ProtoHDFS.Response.Builder responseBuilder, String responseId,
                                                   ProtoHDFS.Response.ResponseType responseType,
                                                   ProtoHDFS.FileHandle fileHandle, ProtoHDFS.Block block,
                                                   String errorMessage){
        responseBuilder.setResponseId(responseId);
        responseBuilder.setResponseType(responseType);
        if(fileHandle != null) responseBuilder.setFileHandle(fileHandle);
        if(block != null) responseBuilder.setBlock(block);
        if(errorMessage != null) responseBuilder.setErrorMessage(errorMessage);
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();
        return response;
    }


}
