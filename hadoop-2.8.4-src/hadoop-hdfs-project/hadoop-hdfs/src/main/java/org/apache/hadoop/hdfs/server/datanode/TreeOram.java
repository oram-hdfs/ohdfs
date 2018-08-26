package org.apache.hadoop.hdfs.server.datanode;
import java.io.IOException;
import java.util.*;
import  java.lang.*;

class TreeOram {
	private static TreeOram instance=new TreeOram();
	public TreeNode root;
	public static TreeOram getInstance(){
		return instance;
	}
	public TreeOram(){
		int i=0,j=0;
		Queue<TreeNode> queue = new LinkedList<TreeNode>();
		root=new TreeNode();
		queue.offer(root);
		while(!queue.isEmpty() && i<=1023){
			TreeNode temp=queue.poll();
			temp.setLeft(new TreeNode());
			temp.getLeft().setParent(temp);
			queue.offer(temp.getLeft());
			temp.setRight(new TreeNode());
			temp.getRight().setParent(temp);
			queue.offer(temp.getRight());
			i+=2;
		 }
	}
	public int getOffset(long blockId){
		TreeNode p=this.root;
		Stack<TreeNode> stack=new Stack<TreeNode>();
		while(p!=null || !stack.empty()){
			if(p!=null){
				stack.push(p);
				List<Slot> slots=p.getSlots();
				Iterator iterator=slots.iterator();
				while(iterator.hasNext()){
					Slot slot=(Slot)iterator.next();
					if(slot.getBlockId()==blockId){
						return slot.getOffset();
					}
				}
			}
			else{
				p=stack.pop();
				p=p.getRight();
			}
		}
		return 0;
	}
	public boolean isinTree(long blockId){
		TreeNode p=this.root;
		Stack<TreeNode> stack=new Stack<TreeNode>();
		while(p!=null || !stack.empty()){
			if(p!=null){
				stack.push(p);
				List<Slot> slots=p.getSlots();
				Iterator iterator=slots.iterator();
				while(iterator.hasNext()){
					Slot slot=(Slot)iterator.next();
					if(slot.getBlockId()==blockId){
						return true;
					}
				}
			}
			else{
				p=stack.pop();
				p=p.getRight();
			}
		}
		return false;
	}
	public void join(long blockId){
		TreeNode p=this.root;
		Slot slot=p.findSlot();
		slot.setBlockId(blockId);
	}
	/*
	public TreeNode getleafNode(int blockid){
		return new TreeNode();
	}
	public TreeNode getrootNode(int blockid){
		return new TreeNode();
	}

	public int findaddr(int blockid,TreeNode leaf){
		int addr = 0;
		TreeNode cur=leaf;
		while(cur!=null){
			for(slot s:cur.slotlist){
				if (s.blockid==blockid){
					addr=s.blockadr;
				}
			}
			cur=cur.parent;
			}
		return addr;
	}
	
	public TreeNode getchidnode(int blockid){
		TreeNode p=null,leafnode=getleafNode(blockid);
		TreeNode cur=leafnode;
		while(!cur.hasblock(blockid)){
			p=cur;
			cur=cur.parent;
		}
		return p;
		
	}
	public TreeNode getlast(){
		return new TreeNode();
	}
	public void decline(TreeNode node){}
	public ArrayList randomTreenode(int range){
		Random rand =new Random(25);
		ArrayList list=new ArrayList();
		while(list.size()<(range/2)+1){
			int num=rand.nextInt(range);
			if(!list.contains(num)){
				list.add(num);
			}
			
		}
		return list;
		} 
	
	public void eviction(){
		Queue<TreeNode> queue = new LinkedList<TreeNode>();
		queue.offer(root);
		int ceng=0;
		TreeNode last=root;ArrayList<TreeNode> nodes=new ArrayList<TreeNode>();
		while(!queue.isEmpty()){
			TreeNode temp=queue.poll();
			nodes.add(temp);
			if(temp.left!=null){
				queue.offer(temp.left);
			}
			if(temp.right!=null){
				queue.offer(temp.right);
			}
			if(temp==last){
				ArrayList random=randomTreenode((int)Math.pow(2,ceng));
				int i=0;
				for(TreeNode node:nodes){
					if (random.contains(i)){
						decline(node);
					}
					i+=1;
				}
				nodes.clear();
			}
		}
		
	}
	*/
	

}
