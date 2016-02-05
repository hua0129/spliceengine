package com.splicemachine.storage;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author Scott Fines
 *         Date: 1/18/16
 */
public class MemFileSystem extends DistributedFileSystem{
    private final FileSystemProvider localDelegate;

    public MemFileSystem(FileSystemProvider localDelegate){
        this.localDelegate=localDelegate;
    }

    @Override
    public void delete(Path path,boolean recursive) throws IOException{
        //TODO -sf- deal with recursive deletes
        localDelegate.delete(path);
    }

    @Override
    public Path getPath(String directory,String fileName){
        return Paths.get(directory,fileName);
    }

    @Override
    public Path getPath(String fullPath){
        return Paths.get(fullPath);
    }

    @Override
    public FileInfo getInfo(String filePath){
        Path p = getPath(filePath);
        return new PathInfo(p);
    }


    /*delegate methods*/
    @Override
    public String getScheme(){
        return localDelegate.getScheme();
    }

    @Override
    public FileSystem newFileSystem(URI uri,Map<String, ?> env) throws IOException{
        return localDelegate.newFileSystem(uri,env);
    }

    @Override
    public FileSystem getFileSystem(URI uri){
        return localDelegate.getFileSystem(uri);
    }

    @Override
    public Path getPath(URI uri){
        return localDelegate.getPath(uri);
    }

    @Override
    public FileSystem newFileSystem(Path path,Map<String, ?> env) throws IOException{
        return localDelegate.newFileSystem(path,env);
    }

    @Override
    public InputStream newInputStream(Path path,OpenOption... options) throws IOException{
        return localDelegate.newInputStream(path,options);
    }

    @Override
    public OutputStream newOutputStream(Path path,OpenOption... options) throws IOException{
        return localDelegate.newOutputStream(path,options);
    }

    @Override
    public FileChannel newFileChannel(Path path,Set<? extends OpenOption> options,FileAttribute<?>... attrs) throws IOException{
        return localDelegate.newFileChannel(path,options,attrs);
    }

    @Override
    public AsynchronousFileChannel newAsynchronousFileChannel(Path path,Set<? extends OpenOption> options,ExecutorService executor,FileAttribute<?>... attrs) throws IOException{
        return localDelegate.newAsynchronousFileChannel(path,options,executor,attrs);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,Set<? extends OpenOption> options,FileAttribute<?>... attrs) throws IOException{
        return localDelegate.newByteChannel(path,options,attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir,DirectoryStream.Filter<? super Path> filter) throws IOException{
        return localDelegate.newDirectoryStream(dir,filter);
    }


    @Override
    public boolean createDirectory(Path dir,boolean errorIfExists) throws IOException{
        try{
            localDelegate.createDirectory(dir);
            return true;
        }catch(FileAlreadyExistsException fafe){
            if(errorIfExists || !Files.isDirectory(dir)){
                throw fafe;
            }
            return false;
        }
    }

    @Override
    public void createDirectory(Path dir,FileAttribute<?>... attrs) throws IOException{
        try{
            localDelegate.createDirectory(dir,attrs);
        }catch(FileAlreadyExistsException fafe){
            //determine if the path is already a directory, or if it is a file. If it's a file, then
            //throw a NotADirectoryException. Otherwise, we are good
            if(!Files.isDirectory(dir)){
                throw fafe;
            }
        }
    }

    @Override
    public void createSymbolicLink(Path link,Path target,FileAttribute<?>... attrs) throws IOException{
        localDelegate.createSymbolicLink(link,target,attrs);
    }

    @Override
    public void createLink(Path link,Path existing) throws IOException{
        localDelegate.createLink(link,existing);
    }

    @Override
    public void delete(Path path) throws IOException{
        localDelegate.delete(path);
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException{
        return localDelegate.deleteIfExists(path);
    }

    @Override
    public Path readSymbolicLink(Path link) throws IOException{
        return localDelegate.readSymbolicLink(link);
    }

    @Override
    public void copy(Path source,Path target,CopyOption... options) throws IOException{
        localDelegate.copy(source,target,options);
    }

    @Override
    public void move(Path source,Path target,CopyOption... options) throws IOException{
        localDelegate.move(source,target,options);
    }

    @Override
    public boolean isSameFile(Path path,Path path2) throws IOException{
        return localDelegate.isSameFile(path,path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException{
        return localDelegate.isHidden(path);
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException{
        return localDelegate.getFileStore(path);
    }

    @Override
    public void checkAccess(Path path,AccessMode... modes) throws IOException{
        localDelegate.checkAccess(path,modes);
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path,Class<V> type,LinkOption... options){
        return localDelegate.getFileAttributeView(path,type,options);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path,Class<A> type,LinkOption... options) throws IOException{
        return localDelegate.readAttributes(path,type,options);
    }

    @Override
    public Map<String, Object> readAttributes(Path path,String attributes,LinkOption... options) throws IOException{
        return localDelegate.readAttributes(path,attributes,options);
    }

    @Override
    public void setAttribute(Path path,String attribute,Object value,LinkOption... options) throws IOException{
        localDelegate.setAttribute(path,attribute,value,options);
    }

    private static class PathInfo implements FileInfo{
        private final Path p;

        public PathInfo(Path p){
            this.p=p;
        }

        @Override
        public String fileName(){
            return p.getFileName().toString();
        }

        @Override
        public String fullPath(){
            return p.toString();
        }

        @Override
        public boolean isDirectory(){
            return Files.isDirectory(p);
        }

        @Override
        public long fileCount(){
            if(!isDirectory()) return 0l;
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(p)) {
                long count = 0;
                for (Path ignored : directoryStream) {
                    count++;
                }
                return count;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public long spaceConsumed(){
            try{
                return Files.size(p);
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isReadable(){
            return Files.isReadable(p);
        }

        @Override
        public String getUser(){
            try{
                return Files.getOwner(p).getName();
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getGroup(){
            throw new UnsupportedOperationException("IMPLEMENT");
        }

        @Override
        public boolean isWritable(){
            return Files.isWritable(p);
        }
    }
}
