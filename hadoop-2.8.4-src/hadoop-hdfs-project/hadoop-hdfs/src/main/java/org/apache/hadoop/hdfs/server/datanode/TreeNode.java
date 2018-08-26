package org.apache.hadoop.hdfs.server.datanode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class  TreeNode{
    private List<Slot>   Slots=new ArrayList<Slot>(10);
    private TreeNode left;
    private TreeNode right;
    private TreeNode parent;
    public  TreeNode(){
        for(int i=0;i<10;i++)
            Slots.add(new Slot());
        this.left=null;
        this.right=null;
        this.parent=null;
    }

    public void setLeft(TreeNode left) {
        this.left = left;
    }

    public TreeNode getLeft() {
        return left;
    }

    public void setRight(TreeNode right) {
        this.right = right;
    }

    public TreeNode getRight() {
        return right;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public TreeNode getParent() {
        return parent;
    }

    public List<Slot> getSlots() {
        return Slots;
    }
    public Slot findSlot(){
        Iterator iterator=this.Slots.iterator();
        while(iterator.hasNext()){
            Slot slot=(Slot) iterator.next();
            if (slot.getBlockId()==0){
                return slot;
            }
        }
        return null;
    }
}


