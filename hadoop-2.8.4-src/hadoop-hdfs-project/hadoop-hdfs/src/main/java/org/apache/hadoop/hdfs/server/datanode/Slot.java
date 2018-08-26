package org.apache.hadoop.hdfs.server.datanode;


class Slot{
    public static  int index=0;
    private  long  blockId;
    private  int  offset;
    public  Slot(){
        this.blockId=0;
        this.offset=index++;
    }

    public long getBlockId() {
        return blockId;
    }

    public int getOffset() {
        return offset;
    }

    public void setBlockId(long blockId) {
        this.blockId = blockId;
    }
}
