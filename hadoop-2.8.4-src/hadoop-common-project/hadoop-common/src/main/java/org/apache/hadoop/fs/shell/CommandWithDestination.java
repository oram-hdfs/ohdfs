/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.shell;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.PathOperationException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;

/**
 * Provides: argument processing to ensure the destination is valid
 * for the number of source arguments.  A processPaths that accepts both
 * a source and resolved target.  Sources are resolved as children of
 * a destination directory.
 */
abstract class CommandWithDestination extends FsCommand {  
  protected PathData dst;
  private boolean overwrite = false;
  private boolean verifyChecksum = true;
  private boolean writeChecksum = true;
  private boolean lazyPersist = false;
  private boolean direct = false;
  
  //design by kyc
  private boolean safeRead= false;
  private boolean safeWrite = false;
  //design by kyc -end

/**
   * The name of the raw xattr namespace. It would be nice to use
   * XAttr.RAW.name() but we can't reference the hadoop-hdfs project.
   */
  private static final String RAW = "raw.";

  /**
   * The name of the reserved raw directory.
   */
  private static final String RESERVED_RAW = "/.reserved/raw";

  /**
   * 
   * This method is used to enable the force(-f)  option while copying the files.
   * 
   * @param flag true/false
   */
  protected void setOverwrite(boolean flag) {
    overwrite = flag;
  }
  
  protected void setLazyPersist(boolean flag) {
    lazyPersist = flag;
  }

  protected void setVerifyChecksum(boolean flag) {
    verifyChecksum = flag;
  }
  
  protected void setWriteChecksum(boolean flag) {
    writeChecksum = flag;
  }

  protected void setDirectWrite(boolean flag) {
    direct = flag;
  }
  
  // add by kyc
  protected void setSafeRead(boolean flag) {
	safeRead = flag;
  }
  protected void setSafeWrite(boolean flag) {
	safeWrite = flag;
  }
  //add by kyc -end

  /**
   * If true, the last modified time, last access time,
   * owner, group and permission information of the source
   * file will be preserved as far as target {@link FileSystem}
   * implementation allows.
   */
  protected void setPreserve(boolean preserve) {
    if (preserve) {
      preserve(FileAttribute.TIMESTAMPS);
      preserve(FileAttribute.OWNERSHIP);
      preserve(FileAttribute.PERMISSION);
    } else {
      preserveStatus.clear();
    }
  }
  
  protected static enum FileAttribute {
    TIMESTAMPS, OWNERSHIP, PERMISSION, ACL, XATTR;

    public static FileAttribute getAttribute(char symbol) {
      for (FileAttribute attribute : values()) {
        if (attribute.name().charAt(0) == Character.toUpperCase(symbol)) {
          return attribute;
        }
      }
      throw new NoSuchElementException("No attribute for " + symbol);
    }
  }
  
  private EnumSet<FileAttribute> preserveStatus = 
      EnumSet.noneOf(FileAttribute.class);
  
  /**
   * Checks if the input attribute should be preserved or not
   *
   * @param attribute - Attribute to check
   * @return boolean true if attribute should be preserved, false otherwise
   */
  private boolean shouldPreserve(FileAttribute attribute) {
    return preserveStatus.contains(attribute);
  }
  
  /**
   * Add file attributes that need to be preserved. This method may be
   * called multiple times to add attributes.
   *
   * @param fileAttribute - Attribute to add, one at a time
   */
  protected void preserve(FileAttribute fileAttribute) {
    for (FileAttribute attribute : preserveStatus) {
      if (attribute.equals(fileAttribute)) {
        return;
      }
    }
    preserveStatus.add(fileAttribute);
  }

  /**
   *  The last arg is expected to be a local path, if only one argument is
   *  given then the destination will be the current directory 
   *  @param args is the list of arguments
   */
  protected void getLocalDestination(LinkedList<String> args)
  throws IOException {
    String pathString = (args.size() < 2) ? Path.CUR_DIR : args.removeLast();
    try {
      dst = new PathData(new URI(pathString), getConf());
      //System.out.println("********CommandWithDestination #getLocalDestination() ,dst --Pathdata was create dst:"+dst+"pathString:"+pathString);
    } catch (URISyntaxException e) {
      if (Path.WINDOWS) {
        // Unlike URI, PathData knows how to parse Windows drive-letter paths.
        dst = new PathData(pathString, getConf());
        
        //System.out.println("******* in CommandWithDestination #getLocalDestination() ,dst --Pathdate was create :"+dst);
      } else {
        throw new IOException("unexpected URISyntaxException", e);
      }
    }
  }

  /**
   *  The last arg is expected to be a remote path, if only one argument is
   *  given then the destination will be the remote user's directory 
   *  @param args is the list of arguments
   *  @throws PathIOException if path doesn't exist or matches too many times 
   */
  protected void getRemoteDestination(LinkedList<String> args)
  throws IOException {
    if (args.size() < 2) {
      dst = new PathData(Path.CUR_DIR, getConf());
      //System.out.println("in CommandWithDestination #getRemoteDestination() ,dst was create :"+dst);
    } else {
    	//System.out.println("in CommandWithDestination # getRemoteDestination() ,dst was not  create :"+dst);
      String pathString = args.removeLast();
      // if the path is a glob, then it must match one and only one path
      PathData[] items = PathData.expandAsGlob(pathString, getConf());
      switch (items.length) {
        case 0:
          throw new PathNotFoundException(pathString);
        case 1:
          dst = items[0];
          break;
        default:
          throw new PathIOException(pathString, "Too many matches");
      }
    }
  }

  @Override
  protected void processArguments(LinkedList<PathData> args)
  throws IOException {
	 //System.out.println(" **********CommandWithDestination #processArguments()  --kyc");

    // if more than one arg, the destination must be a directory
    // if one arg, the dst must not exist or must be a directory
    if (args.size() > 1) {
      if (!dst.exists) {
        throw new PathNotFoundException(dst.toString());
    	 // System.out.println("in processArguments () !dst.exists");
        
      }
      if (!dst.stat.isDirectory()) {
        throw new PathIsNotDirectoryException(dst.toString());
      }
    } else if (dst.exists) {
      if (!dst.stat.isDirectory() && !overwrite) {
    	  
        throw new PathExistsException(dst.toString());
      }
    } else if (!dst.parentExists()) {
      throw new PathNotFoundException(dst.toString())
          .withFullyQualifiedPath(dst.path.toUri().toString());
    }
    super.processArguments(args);
  }

  @Override
  protected void processPathArgument(PathData src)
  throws IOException {
	  //System.out.println("this is process processPathArgument ,kyc");
	  
    if (src.stat.isDirectory() && src.fs.equals(dst.fs)) {
      PathData target = getTargetPath(src);
      String srcPath = src.fs.makeQualified(src.path).toString();
      String dstPath = dst.fs.makeQualified(target.path).toString();
      
     // System.out.println("kyc, processPathArgument(),this is srcPath"+srcPath);
      
      //System.out.println("kyc, processPathArgument(),this is dstPath"+dstPath);
      
      if (dstPath.equals(srcPath)) {
        PathIOException e = new PathIOException(src.toString(),
            "are identical");
        e.setTargetPath(dstPath.toString());
        throw e;
      }
      // When a path is normalized, all trailing slashes are removed
      // except for the root
      if(!srcPath.endsWith(Path.SEPARATOR)) {
        srcPath += Path.SEPARATOR;
      }

      if(dstPath.startsWith(srcPath)) {
        PathIOException e = new PathIOException(src.toString(),
            "is a subdirectory of itself");
        e.setTargetPath(target.toString());
        throw e;
      }
    }
    super.processPathArgument(src);
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    processPath(src, getTargetPath(src));
  }
  
  /**
   * Called with a source and target destination pair
   * @param src for the operation
   * @param dst for the operation
   * @throws IOException if anything goes wrong
   */
  protected void processPath(PathData src, PathData dst) throws IOException {
    if (src.stat.isSymlink()) {
      // TODO: remove when FileContext is supported, this needs to either
      // copy the symlink or deref the symlink
      throw new PathOperationException(src.toString());        
    } else if (src.stat.isFile()) {
    	
//    	String filename = src.toString();
//    	if (filename.indexOf("_safe_")!=-1){
//    		// safe file that need to be cut
//    		System.out.println("this is a safe file");
//    		int numberOfDummy = filename.charAt(filename.length()-1)-48;
//    		copyFileToTarget4(src, dst,numberOfDummy);
//    	}
//    	else{
//    		// do nothing
//    		
//    		System.out.println("this is a un safe file");
//    		copyFileToTarget(src, dst);
//    	}
    	
      //  added by kangyucheng 
    	// this is for read
    	
    	
      if (safeRead){
    	  
    	  
      	//System.out.println(" -s is true done by kangyucheng,so we should do safe write or safe read");
      	//System.out.println("we should analys src and dst,so that we can foregy ");
      	
//      	System.out.println("dst.fs:"+dst.fs);
//      	System.out.println("dst.path:"+dst.path);
//      	System.out.println("dst.stat:"+dst.stat);
//      	
//      	System.out.println("src.fs:"+src.fs);
//      	System.out.println("src.path:"+src.path);
//      	System.out.println("src.stat:"+src.stat);
      	
      	try {
      		//1. we should check if the file in local dummy filepath
      		
      		String srcFilePath = "";
      		//System.out.print("The source file is "+src.toString());
      		
      		if (src.existsInLocal){
      			// 1.1 the file in local,so the srcFilePath is just the src
      			// what we just need to do is copy it to usre's filePath
     
      			copyFileToTarget(src, dst);

      		}
      		else{
      			System.out.println("PLEASE WAIT ....");
      			//1.2 the file is in hdfs,so the srcFilePath
      			String filename = src.toString();
          		srcFilePath = DUMMY+filename;
      			
      		
      		
      				
      	
//      		if  (src_items.contains(srcFilePath)){
//      			// file already exit in local 
//      			PathData storageForSafesrc = new PathData(new URI(srcFilePath), getConf());
//      			copyFileToTarget(storageForSafesrc,dst);
//      		}
//      		else{
//      			
//      		
          		//2.the source has been set, the destination are going to be set.
          		//One destination is dummy filePath, the other is user's filePath 
      		
          		PathData storageForSafeDst = new PathData(new URI(srcFilePath), getConf());
          		copyFileToTarget5forCutAddtionFile(src, dst);	
          		copyFileToTarget5forCutAddtionFile(dst, storageForSafeDst);
          		//3.we should delete the file we have just read from hdfs 
          		src.fs.delete(src.path,true);
          		//4.now we should choose a file in dummy filePath to write to hdfs
      		
     
        		// safe file that need to be cut
//        		System.out.println("this is a safe file");
//        		int numberOfDummy = filename.charAt(filename.length()-1)-48;
        	//	copyFileToTarget4(src, dst,numberOfDummy);
        		
//        		copyFileToTarget4(src, storageForSafeDst,numberOfDummy);
//        	}
//        	else{
        		//safe file that need  not to be cut
        		
//        		System.out.println("this is a un safe file");
        	
        	
        		
//        	}			
//      		String dst_pathString = getReWriteFile();
//      		System.out.println("safe_dst_pathString"+dst_pathString);
      		// if the path is a glob, then it must match one and only one path
      		

        	// the follow is to find dummy file to write to hdfs
          	// the src_items is the list of dummy file
        	List<String> src_items = getAllFile(DUMMY,false);
        	// k is a random integer 
			int k = (int) (Math.random()* (src_items.size()-1));
			// choose a file at random
			String srcpathString = src_items.get(k);
			System.out.println("The dummy file "+srcpathString+" is selected to write to HDFS");
			// safe_src is source safe_dst is destination
			PathData safe_src = new PathData(new URI(srcpathString), getConf());
			PathData safe_dst ;
			String[] dstpaths = srcpathString.split("/");
			String dstpath =dstpaths[dstpaths.length-1];
			
			
			PathData[] items = PathData.expandAsGlob(dstpath, getConf());
			switch (items.length) {
			case 0:
		          throw new PathNotFoundException("safe Wrong");
		    case 1:
		    	safe_dst = items[0];
		        break;
		    default:
		    	throw new PathNotFoundException("safe Wrong");
		    }
			// construct a integer at random to build dummy block 
			int num =(int)(Math.random()*4); 
			// 5.write the dummy file we just chose
		    copyFileToTarget2ForAddBlocks(safe_src,safe_dst,num); // in this method the local file was deleted
		    // 6.delete the file from dummy filePath
		    safe_src.fs.delete(safe_src.path,false);
		    
//			 PathData[] items2 = safe_dst.getDirectoryContents();
//			 for (PathData item:items2){
//					System.out.println("int safe read item:"+item);
//				}
//			 
//			 int k = (int) ((items2.length-1)*Math.random());
//			 safe_dst=items2[k];
//			 
//		    	System.out.println("safe_dst.fs:"+safe_dst.fs);
//		      	System.out.println("safe_dst.path:"+safe_dst.path);
//		      	System.out.println("safe_dst.stat:"+safe_dst.stat);
		    System.out.println("The file "+dstpath+" has been written to HDFS");
     		}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("WE DO NOT FINISH TO GET THE FILE --kangyucheng ");
			//e.printStackTrace();
		}
      	
      }
      // this is  for write
      else if (safeWrite){
    	  try {
    		  System.out.println("PLEASE WAIT ....");
    		  PathData safe_src ;
    
//    	  PathData[] items = PathData.expandAsGlob(".", getConf());
//    
//    	  
//			switch (items.length) {
//				case 0:
//			          throw new PathNotFoundException("Wrong write");
//			    case 1:
//			    	safe_src = items[0];
//			        break;
//			    default:
//			    	throw new PathNotFoundException("Wrong write1");
//			    	}
//			
//			
//			 PathData[] items2 = safe_src.getDirectoryContents();
    		  
    		  
    		  //1. we will get all the filePath in HDFS
    		  PathData[] items2 = getAllDirec(getConf());
			 

    		  //2.we chose one at random
    		  int k = (int) ((items2.length-1)*Math.random());
//			  System.out.println("k is "+k+ " items.length:"+items2.length);
			  safe_src=items2[k]; 
			  System.out.println("The file :"+safe_src+" in HDFS  was chose to read");
			 
//			System.out.println("safe_src.fs:"+safe_src.fs);
//	      	System.out.println("safe_src.path:"+safe_src.path);
//	      	System.out.println("safe_src.stat:"+safe_src.stat);
			 
			 
			  //3.we read it but we do not real write it
			  copyFileToTarget3readExceptWrite(safe_src,dst);
	      	
	      	
	      	/// nolmoral 
//	      	String filename = src.toString();
//	    	if (filename.indexOf("_safe_")!=-1){
//	    		// safe file that need to be cut
//	    		System.out.println("this is a safe file");
//	    		int numberOfDummy = filename.charAt(filename.length()-1)-48;
//	    		
//	    	}
//	    	else{
//	    		// do nothing
	    		
//	    		System.out.println("this is a un safe file");
	    		//copyFileToTarget(src, dst);
//	    		
	    		//copyFileToTarget5forCutAddtionFile(safe_src,dst);
//	    	}
			  
			  // 4 .check if the file in dummy filePath
			  String tem = dst.toString();
			  String srcFilePath =DUMMY+tem;
			  PathData localFile =null;
			  try {
					localFile = new PathData(new URI(srcFilePath), getConf());
					if (localFile.exists){	
						localFile.existsInLocal=true;
					}
			  //check local end 
			  } catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			  }
			  
			  if (localFile.existsInLocal){
				  System.out.println("The File has alread exist in Local,you sholud not put it!!!");
			  }
			  else{
				  int num =(int)(Math.random()*4);  	
		    	  copyFileToTarget2ForAddBlocks(src,dst,num);
			  }
			  
    	  } catch (Exception e) {
  			// TODO Auto-generated catch block
    	 System.out.println("WE DO NOT FINISH TO GET THE FILE --kangyucheng ");
//  			e.printStackTrace();
  		}
    
      }
      else{
    	  System.out.println("UNSAFE COMMOND --kangyucheng");
//      	if (filename.indexOf("_safe_")!=-1){
      		// safe file that need to be cut
//      		System.out.println("this is a safe file");
//      		int numberOfDummy = filename.charAt(filename.length()-1)-48;
    	  
    	  try {
    		  copyFileToTarget5forCutAddtionFile(src, dst);
		  //check local end 
		  } catch (Exception e) {
				// TODO Auto-generated catch block
			  System.out.println("THERE IS SOMETHING WRONG ! --kangyucheng");
			 
//				e.printStackTrace();
		  }
//      	}
//      	else{
      		// do nothing
      		
//      		System.out.println("this is a un safe file");
//      		copyFileToTarget(src, dst);
//      	}
      }
       
      //add by kangyucheng  end 
      
      
    } else if (src.stat.isDirectory() && !isRecursive()) {
      throw new PathIsDirectoryException(src.toString());
    }
  }
  ///////////////////////add by kangyucheng start//////////////////////////////
  /**
   *  get all filePath in HDFS
   *  add by kangyucheng
   *  @return PathData[]  
   */
  public static PathData[] getAllDirec(Configuration con){
	  PathData safe_src ;
	    
	  PathData[] items,items2 = null;
	  try {
		  items = PathData.expandAsGlob(".", con);
		  switch (items.length) {
		  	  case 0:
		  		  throw new PathNotFoundException("Wrong write");
			  case 1:
				  safe_src = items[0];
				  break;
			  default:
				  throw new PathNotFoundException("Wrong write1");
	    		}
	
		items2 = safe_src.getDirectoryContents(); 
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return items2;
		
  }
  /**
   *  get all filePath Dummy filePath
   *  add by kangyucheng
   *  @return List<String>
   */
  
  public static List<String> getAllFile(String directoryPath,boolean isAddDirectory) {
      List<String> list = new ArrayList<String>();
      File baseFile = new File(directoryPath);
      if (baseFile.isFile() || !baseFile.exists()) {
          return list;
      }
      File[] files = baseFile.listFiles();
      for (File file : files) {
          if (file.isDirectory()) {
              if(isAddDirectory){
                  list.add(file.getAbsolutePath());
              }
              list.addAll(getAllFile(file.getAbsolutePath(),isAddDirectory));
          } else {
              list.add(file.getAbsolutePath());
          }
      }
      return list;
  }
  ///////////////// add by kangyucheng end///////////////////////////////
  
  
  @Override
  protected void recursePath(PathData src) throws IOException {
    PathData savedDst = dst;
    try {
      // modify dst as we descend to append the basename of the
      // current directory being processed
      dst = getTargetPath(src);
      final boolean preserveRawXattrs =
          checkPathsForReservedRaw(src.path, dst.path);
      if (dst.exists) {
        if (!dst.stat.isDirectory()) {
          throw new PathIsNotDirectoryException(dst.toString());
        }
      } else {
        if (!dst.fs.mkdirs(dst.path)) {
          // too bad we have no clue what failed
          PathIOException e = new PathIOException(dst.toString());
          e.setOperation("mkdir");
          throw e;
        }    
        dst.refreshStatus(); // need to update stat to know it exists now
      }      
      super.recursePath(src);
      if (dst.stat.isDirectory()) {
        preserveAttributes(src, dst, preserveRawXattrs);
      }
    } finally {
      dst = savedDst;
    }
  }
  
  protected PathData getTargetPath(PathData src) throws IOException {
    PathData target;
    // on the first loop, the dst may be directory or a file, so only create
    // a child path if dst is a dir; after recursion, it's always a dir
    if ((getDepth() > 0) || (dst.exists && dst.stat.isDirectory())) {
      target = dst.getPathDataForChild(src);
    } else if (dst.representsDirectory()) { // see if path looks like a dir
      target = dst.getPathDataForChild(src);
    } else {
      target = dst;
    }
    return target;
  }
  
  /**
   * Copies the source file to the target.
   * @param src item to copy
   * @param target where to copy the item
   * @throws IOException if copy fails
   */ 
	protected void copyFileToTarget(PathData src, PathData target)
      throws IOException {
    final boolean preserveRawXattrs =
        checkPathsForReservedRaw(src.path, target.path);
  
    src.fs.setVerifyChecksum(verifyChecksum);
    InputStream in = null;
    try {
      in = src.fs.open(src.path);
      copyStreamToTarget(in, target);
      
      preserveAttributes(src, target, preserveRawXattrs);
    } finally {
      IOUtils.closeStream(in);
    }
  }
	
	
	

	
///////////////////////////////////////add by kyc start ////////////////////////////////////
	/**
	   * add by kangyucheng   
	   * @throws IOException if copy fails
	   */ 
		protected void copyFileToTarget2ForAddBlocks(PathData src,PathData safe_target,int num)
	      throws IOException {
	    final boolean preserveRawXattrs =
	        checkPathsForReservedRaw(src.path, safe_target.path);
	    
	    //System.out.println("*************copyFileToTarget2, \n src is "+src+"\n target is "+target);
	    
	    src.fs.setVerifyChecksum(verifyChecksum);
	    InputStream in = null;
	    try {
	      //System.out.println("src.path"+src.path);
	      in = src.fs.open(src.path);
	      copyStreamToTarget2(in, safe_target,num);
	      preserveAttributes(src, safe_target, preserveRawXattrs);
	    } finally {
	      IOUtils.closeStream(in);
	    }
	  }
		
		protected void copyFileToTarget3readExceptWrite(PathData safe_src, PathData target)
			      throws IOException {
			    final boolean preserveRawXattrs =
			        checkPathsForReservedRaw(safe_src.path, target.path);
			    
			  //  System.out.println("*************copyFileToTarget3, \n safe_src is "+safe_src+"\n target is "+target);
			    
			    safe_src.fs.setVerifyChecksum(verifyChecksum);
			    InputStream in = null;
			    try {
			      in = safe_src.fs.open(safe_src.path);
			      copyStreamToTarget3(in, target);
			      
			      preserveAttributes(safe_src, target, preserveRawXattrs);
			    } finally {
			      IOUtils.closeStream(in);
			    }
			  }
//		protected void copyFileToTarget4(PathData src, PathData target,int numberOfDummy)
//			      throws IOException {
//			    final boolean preserveRawXattrs =
//			        checkPathsForReservedRaw(src.path, target.path);
//			    
//			   // System.out.println("*************copyFileToTarget, \n src is "+src+"\n target is "+target);
//			    
//			    src.fs.setVerifyChecksum(verifyChecksum);
//			    InputStream in = null;
//			    try {
//			      in = src.fs.open(src.path);
//			      copyStreamToTarget4(in, target,numberOfDummy);
//			      
//			      preserveAttributes(src, target, preserveRawXattrs);
//			    } finally {
//			      IOUtils.closeStream(in);
//			    }
//			  }
//		
		protected void copyFileToTarget5forCutAddtionFile(PathData src, PathData target)
			      throws IOException {
			    final boolean preserveRawXattrs =
			        checkPathsForReservedRaw(src.path, target.path);
			    
			   // System.out.println("*************copyFileToTarget, \n src is "+src+"\n target is "+target);
			    
			    src.fs.setVerifyChecksum(verifyChecksum);
			    InputStream in = null;
			    try {
			      in = src.fs.open(src.path);
			      copyStreamToTarget5(in, target);
			      
			      preserveAttributes(src, target, preserveRawXattrs);
			    } finally {
			      IOUtils.closeStream(in);
			    }
			  }
		//////////////////////////////add by kangyucheng////////////////////////////////////////
		
		
		

  /**
   * Check the source and target paths to ensure that they are either both in
   * /.reserved/raw or neither in /.reserved/raw. If neither src nor target are
   * in /.reserved/raw, then return false, indicating not to preserve raw.*
   * xattrs. If both src/target are in /.reserved/raw, then return true,
   * indicating raw.* xattrs should be preserved. If only one of src/target is
   * in /.reserved/raw then throw an exception.
   *
   * @param src The source path to check. This should be a fully-qualified
   *            path, not relative.
   * @param target The target path to check. This should be a fully-qualified
   *               path, not relative.
   * @return true if raw.* xattrs should be preserved.
   * @throws PathOperationException is only one of src/target are in
   * /.reserved/raw.
   */
  private boolean checkPathsForReservedRaw(Path src, Path target)
      throws PathOperationException {
    final boolean srcIsRR = Path.getPathWithoutSchemeAndAuthority(src).
        toString().startsWith(RESERVED_RAW);
    final boolean dstIsRR = Path.getPathWithoutSchemeAndAuthority(target).
        toString().startsWith(RESERVED_RAW);
    boolean preserveRawXattrs = false;
    if (srcIsRR && !dstIsRR) {
      final String s = "' copy from '" + RESERVED_RAW + "' to non '" +
          RESERVED_RAW + "'. Either both source and target must be in '" +
          RESERVED_RAW + "' or neither.";
      throw new PathOperationException("'" + src.toString() + s);
    } else if (!srcIsRR && dstIsRR) {
      final String s = "' copy from non '" + RESERVED_RAW +"' to '" +
          RESERVED_RAW + "'. Either both source and target must be in '" +
          RESERVED_RAW + "' or neither.";
      throw new PathOperationException("'" + dst.toString() + s);
    } else if (srcIsRR && dstIsRR) {
      preserveRawXattrs = true;
    }
    return preserveRawXattrs;
  }

  /**
   * If direct write is disabled ,copies the stream contents to a temporary
   * file "<target>._COPYING_". If the copy is
   * successful, the temporary file will be renamed to the real path,
   * else the temporary file will be deleted.
   * if direct write is enabled , then creation temporary file is skipped.
   * @param in the input stream for the copy
   * @param target where to store the contents of the stream
   * @throws IOException if copy fails
   */ 
  protected void copyStreamToTarget(InputStream in, PathData target)
  throws IOException {
	//System.out.println("**************copyStreamToTarget() by kangyucheng  # start");
    if (target.exists && (target.stat.isDirectory() || !overwrite)) {
      throw new PathExistsException(target.toString());
    }
    TargetFileSystem targetFs = new TargetFileSystem(target.fs);
    try {
    	
     // System.out.println("copyStreamToTarget 's try  by kangyucheng");
      PathData tempTarget = direct ? target : target.suffix("._COPYING_");
      targetFs.setWriteChecksum(writeChecksum);
      targetFs.writeStreamToFile(in, tempTarget, lazyPersist, true);
      if (!direct) {
        targetFs.rename(tempTarget, target);
      }
    } finally {
      targetFs.close(); // last ditch effort to ensure temp file is removed
    }
   // System.out.println("**************copyStreamToTarget() by kangyucheng  # end");
  }

  
  /////////////////////////////add by kyc start//////////////////////
  /**	at org.apache.hadoop.fs.shell.CommandWithDestination.copyStreamToTarget5(CommandWithDestination.java:929)

   * If direct write is disabled ,copies the stream contents to a temporary
   * file "<target>._COPYING_". If the copy is
   * successful, the temporary file will be renamed to the real path,
   * else the temporary file will be deleted.
   * if direct write is enabled , then creation temporary file is skipped.
   * @param in the input stream for the copy
   * @param target where to store the contents of the stream
   * @throws IOException if copy fails
   */ 
  protected void copyStreamToTarget2(InputStream in, PathData safe_target,int num)
  throws IOException {
	//System.out.println("**************copyStreamToTarget()2 by kangyucheng  # start");
    if (safe_target.exists && (safe_target.stat.isDirectory() || !overwrite)) {
      throw new PathExistsException(safe_target.toString());
    }
    
    //TargetFileSystem targetFs = new TargetFileSystem(target.fs);
    TargetFileSystem safeTargetFS = new TargetFileSystem(safe_target.fs);
    try {
    	
//      System.out.println("copyStreamToTarget2 's try  by kangyuc heng");
     // PathData tempTarget = direct ? target : target.suffix("._COPYING_");
    
      //PathData tempSafe_target = direct ? safe_target : safe_target.suffix("._COPYING_");
      safeTargetFS.setWriteChecksum(writeChecksum);
      safeTargetFS.writeStreamToFile2(in, lazyPersist, direct,safe_target,num);
      if (!direct) {
        //targetFs.rename(tempTarget, target);
        //safeTargetFS.rename(tempSafe_target, safe_target);
      }
    } finally {
      //targetFs.close(); // last ditch effort to ensure temp file is removed
      safeTargetFS.close();//add by kangyucheng
    }
//    System.out.println("**************copyStreamToTarget2() by kangyucheng  # end");
  }
  /**
   * If direct write is disabled ,copies the stream contents to a temporary
   * file "<target>._COPYING_". If the copy is
   * successful, the temporary file will be renamed to the real path,
   * else the temporary file will be deleted.
   * if direct write is enabled , then creation temporary file is skipped.
   * @param in the input stream for the copy
   * @param target where to store the contents of the stream
   * @throws IOException if copy fails
   */ 
  protected void copyStreamToTarget3(InputStream in, PathData target)
  throws IOException {
//	System.out.println("**************copyStreamToTarget3() by kangyucheng  # start");
    if (target.exists && (target.stat.isDirectory() || !overwrite)) {
      throw new PathExistsException(target.toString());
    }
    TargetFileSystem targetFs = new TargetFileSystem(target.fs);
    try {
    	
//      System.out.println("copyStreamToTarget3 's try  by kangyucheng");
      //PathData tempTarget = direct ? target : target.suffix("._COPYING_");
      targetFs.setWriteChecksum(writeChecksum);
      targetFs.writeStreamToFile3(in, target, lazyPersist, true);
      if (!direct) {
        //targetFs.rename(tempTarget, target);
      }
    } finally {
      targetFs.close(); // last ditch effort to ensure temp file is removed
    }
//    System.out.println("**************copyStreamToTarget3() by kangyucheng  # end");
  }

//  protected void copyStreamToTarget4(InputStream in, PathData target,int numberOfDummy)
//		  throws IOException {
////			System.out.println("**************copyStreamToTarget3() by kangyucheng  # start");
//		    if (target.exists && (target.stat.isDirectory() || !overwrite)) {
//		      throw new PathExistsException(target.toString());
//		    }
//		    TargetFileSystem targetFs = new TargetFileSystem(target.fs);
//		    try {
//		    	
////		      System.out.println("copyStreamToTarget3 's try  by kangyucheng");
//		      //PathData tempTarget = direct ? target : target.suffix("._COPYING_");
//		      targetFs.setWriteChecksum(writeChecksum);
//		      targetFs.writeStreamToFile4(in, target, lazyPersist, true,numberOfDummy);
//		      if (!direct) {
//		        //targetFs.rename(tempTarget, target);
//		      }
//		    } finally {
//		      targetFs.close(); // last ditch effort to ensure temp file is removed
//		    }
////		    System.out.println("**************copyStreamToTarget3() by kangyucheng  # end");
//		  }
  
  protected void copyStreamToTarget5(InputStream in, PathData target)
  throws IOException {
	//System.out.println("**************copyStreamToTarget() by kangyucheng  # start");
    if (target.exists && (target.stat.isDirectory() || !overwrite)) {
      throw new PathExistsException(target.toString());
    }
    TargetFileSystem targetFs = new TargetFileSystem(target.fs);
    try {
    	
     // System.out.println("copyStreamToTarget 's try  by kangyucheng");
     // PathData tempTarget = direct ? target : target.suffix("._COPYING_");
      targetFs.setWriteChecksum(writeChecksum);
      targetFs.writeStreamToFile5(in, target, lazyPersist, true);
      if (!direct) {
//        targetFs.rename(tempTarget, target);
      }
    } finally {
      targetFs.close(); // last ditch effort to ensure temp file is removed
    }
   // System.out.println("**************copyStreamToTarget() by kangyucheng  # end");
  }
  
  /////////////////////////////add by kyc end//////////////////////
  /**
   * Preserve the attributes of the source to the target.
   * The method calls {@link #shouldPreserve(FileAttribute)} to check what
   * attribute to preserve.
   * @param src source to preserve
   * @param target where to preserve attributes
   * @param preserveRawXAttrs true if raw.* xattrs should be preserved
   * @throws IOException if fails to preserve attributes
   */
  protected void preserveAttributes(PathData src, PathData target,
      boolean preserveRawXAttrs)
      throws IOException {
    if (shouldPreserve(FileAttribute.TIMESTAMPS)) {
      target.fs.setTimes(
          target.path,
          src.stat.getModificationTime(),
          src.stat.getAccessTime());
    }
    if (shouldPreserve(FileAttribute.OWNERSHIP)) {
      target.fs.setOwner(
          target.path,
          src.stat.getOwner(),
          src.stat.getGroup());
    }
    if (shouldPreserve(FileAttribute.PERMISSION) ||
        shouldPreserve(FileAttribute.ACL)) {
      target.fs.setPermission(
          target.path,
          src.stat.getPermission());
    }
    if (shouldPreserve(FileAttribute.ACL)) {
      FsPermission perm = src.stat.getPermission();
      if (perm.getAclBit()) {
        List<AclEntry> srcEntries =
            src.fs.getAclStatus(src.path).getEntries();
        List<AclEntry> srcFullEntries =
            AclUtil.getAclFromPermAndEntries(perm, srcEntries);
        target.fs.setAcl(target.path, srcFullEntries);
      }
    }
    final boolean preserveXAttrs = shouldPreserve(FileAttribute.XATTR);
    if (preserveXAttrs || preserveRawXAttrs) {
      Map<String, byte[]> srcXAttrs = src.fs.getXAttrs(src.path);
      if (srcXAttrs != null) {
        Iterator<Entry<String, byte[]>> iter = srcXAttrs.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<String, byte[]> entry = iter.next();
          final String xattrName = entry.getKey();
          if (xattrName.startsWith(RAW) || preserveXAttrs) {
            target.fs.setXAttr(target.path, entry.getKey(), entry.getValue());
          }
        }
      }
    }
  }

  // Helper filter filesystem that registers created files as temp files to
  // be deleted on exit unless successfully renamed
  private static class TargetFileSystem extends FilterFileSystem {
    TargetFileSystem(FileSystem fs) {
      super(fs);
//      System.out.println("TargetFileSystem by kangyucheng");
    }

    void writeStreamToFile(InputStream in, PathData target,
        boolean lazyPersist, boolean direct)
        throws IOException {
//    	System.out.println("************TargetFileSystem . # writeStreamToFile() ");
      FSDataOutputStream out = null;
      try {
        out = create(target, lazyPersist, direct);
//        System.out.println("in TargetFileSystem() try {} out :" +out);
        IOUtils.copyBytes(in, out, getConf(), true);
      } finally {
        IOUtils.closeStream(out); // just in case copyBytes didn't
      }
    }
    ///////////////////////////add by kyc start////////////////////////////////////

    void writeStreamToFile2(InputStream in,
            boolean lazyPersist, boolean direct, PathData safe_target,int num)
            throws IOException {
//        	System.out.println("************TargetFileSystem . # writeStreamToFile2() ");
          //FSDataOutputStream out = null;
          FSDataOutputStream safe_out = null;
          try {
            //out = create(target, lazyPersist, direct);
      
            safe_out = create(safe_target,lazyPersist, direct);
           // int num=(int)(Math.random()*5);
            IOUtils.copyBytes2(in,getConf(), true, safe_out,num);
            
          } finally {
           
        //IOUtils.closeStream(out); // just in case copyBytes didn't
        	  
           IOUtils.closeStream(safe_out);
           
//           src.fs.delete(src.path,false);
            // Trash.moveToAppropriateTrash(src.fs, src.path, getConf());
          //safe_target.fs.rename(safe_target.path, src.path);
            
          }
        }
    void writeStreamToFile3(InputStream in, PathData target,
            boolean lazyPersist, boolean direct)
            throws IOException {
//        	System.out.println("************TargetFileSystem . # writeStreamToFile3() ");
          FSDataOutputStream out = null;
          try {
            //out = create(target, lazyPersist, direct);
//            System.out.println("in TargetFileSystem() try {} out :" +out);
            IOUtils.copyBytes3(in, out, getConf(), true);
          } finally {
            IOUtils.closeStream(out); // just in case copyBytes didn't
            
            //target.fs.delete(target.path,true);
          }
    }
//    void writeStreamToFile4(InputStream in, PathData target,
//            boolean lazyPersist, boolean direct,int numberOfDummy)
//            throws IOException {
////        	System.out.println("************TargetFileSystem . # writeStreamToFile3() ");
//          FSDataOutputStream out = null;
//          try {
//            //out = create(target, lazyPersist, direct);
////            System.out.println("in TargetFileSystem() try {} out :" +out);
//            IOUtils.copyBytes4(in, out, getConf(), true,numberOfDummy);
//          } finally {
//             // just in case copyBytes didn't
//            //add by kyc
//            PathData temTarget = target;
//            String p = target.path.toString();
//            String q = p.substring(0, p.indexOf("_safe_"));
//            URI uri = PathData.stringToUri(q);
//            temTarget.path=target.fs.makeQualified(new Path(uri));
//            target.fs.rename(target.path,temTarget.path );
//            //add by kyc -end
//            
//            IOUtils.closeStream(out);
//            //target.fs.rename(target, temTarget);
//          }
//    }
    
    void writeStreamToFile5(InputStream in, PathData target,
            boolean lazyPersist, boolean direct)
            throws IOException {
//        	System.out.println("************TargetFileSystem . # writeStreamToFile() ");
          FSDataOutputStream out = null;
          try {
            out = create(target, lazyPersist, direct);
//            System.out.println("in TargetFileSystem() try {} out :" +out);
            IOUtils.copyBytes5(in, out, getConf(), true);
          } finally {
            IOUtils.closeStream(out); // just in case copyBytes didn't
          }
        }
    ////////////////////////////////////////////add by kyc end///////////////////////////////////
    
    
    // tag created files as temp files
    FSDataOutputStream create(PathData item, boolean lazyPersist,
        boolean direct)
        throws IOException {
      try {
        if (lazyPersist) {
          EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, LAZY_PERSIST);
          return create(item.path,
                        FsPermission.getFileDefault().applyUMask(
                            FsPermission.getUMask(getConf())),
                        createFlags,
                        getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
                            IO_FILE_BUFFER_SIZE_DEFAULT),
                        lazyPersist ? 1 : getDefaultReplication(item.path),
                        getDefaultBlockSize(),
                        null,
                        null);
        } else {
        //	System.out.println("item.path:"+item.path+" ");
          return create(item.path, true);
        }
      } finally { // might have been created but stream was interrupted
        if (!direct) {
          //deleteOnExit(item.path);
        }
      }
    }

    void rename(PathData src, PathData target) throws IOException {
      // the rename method with an option to delete the target is deprecated
      if (target.exists && !delete(target.path, false)) {
        // too bad we don't know why it failed
        PathIOException e = new PathIOException(target.toString());
        e.setOperation("delete");
        throw e;
      }
      if (!rename(src.path, target.path)) {
        // too bad we don't know why it failed
        PathIOException e = new PathIOException(src.toString());
        e.setOperation("rename");
        e.setTargetPath(target.toString());
        throw e;
      }
      // cancel delete on exit if rename is successful
      cancelDeleteOnExit(src.path);
    }
    @Override
    public void close() {
      // purge any remaining temp files, but don't close underlying fs
      processDeleteOnExit();
    }
  }
}
