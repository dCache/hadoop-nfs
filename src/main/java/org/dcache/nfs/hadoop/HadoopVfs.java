/*
 * Copyright (c) 2009 - 2012 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.nfs.hadoop;

import org.dcache.chimera.nfs.vfs.VirtualFileSystem;

import com.google.common.cache.*;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.dcache.chimera.nfs.v4.xdr.nfsace4;
import org.dcache.chimera.nfs.vfs.Inode.Type;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.nfs.vfs.DirectoryEntry;
import org.dcache.chimera.nfs.vfs.FsStat;
import org.dcache.chimera.nfs.vfs.Inode;
import org.dcache.chimera.posix.Stat;
import org.dcache.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop based VFS implementation
 */
public class HadoopVfs implements VirtualFileSystem {

    private final static Logger _log = LoggerFactory.getLogger(HadoopVfs.class);
    private final static String NFS_PREFIX = "/";
    private final FileSystem _fs;
    private final BiMap<Path, UUID> _inodeMap = HashBiMap.create();
    private final Cache<Path, FSDataInputStream> _inDescriptorCache;

    public HadoopVfs(String nameNode) throws IOException {
        Configuration conf = new Configuration(true);
        conf.set("fs.default.name", nameNode);

        _fs = FileSystem.get(conf);
        _inDescriptorCache = CacheBuilder.newBuilder().removalListener(new HadoopFileCloser()).build(new HadoopFileOpener());
    }

    @Override
    public Inode create(Inode parent, Type type, String path, int uid, int gid, int mode) throws IOException {
        HadoopInode hinode = (HadoopInode) parent;
        Path p = new Path(hinode.getPath(), path);

        if (_fs.createNewFile(p)) {
            return new HadoopInode(p);
        }
        if (_fs.exists(p)) {
            throw new FileExistsChimeraFsException();
        }
        throw new IOException();
    }

    @Override
    public FsStat getFsStat() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Inode getRootInode() throws IOException {
        return new HadoopInode(new Path(NFS_PREFIX));
    }

    @Override
    public Inode inodeOf(byte[] fh) {
        UUID uuid = toUUID(fh);
        Path p = pathOf(uuid);
        return new HadoopInode(p);
    }

    @Override
    public Inode inodeOf(Inode parent, String path) throws IOException {

        HadoopInode hinode = (HadoopInode) parent;
        Path p = new Path(hinode.getPath(), path);
        if (!_fs.exists(p)) {
            throw new FileNotFoundHimeraFsException();
        }
        return new HadoopInode(p);
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, int uid, int gid) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        List<DirectoryEntry> list = new ArrayList<DirectoryEntry>();

        HadoopInode hinode = (HadoopInode) inode;
        Path p = hinode.getPath();

        for (FileStatus status : _fs.listStatus(p)) {
            String name = status.getPath().getName();
            HadoopInode finode = new HadoopInode(status.getPath());
            list.add(new DirectoryEntry(name, finode));
        }

        return list;
    }

    @Override
    public Inode mkdir(Inode parent, String path, int uid, int gid, int mode) throws IOException {
        HadoopInode hinode = (HadoopInode) parent;
        Path p = new Path(hinode.getPath(), path);
        _fs.mkdirs(p);
        return new HadoopInode(p);
    }

    @Override
    public void move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {

        HadoopInode hinode = (HadoopInode) inode;
        Path p = hinode.getPath().getParent();
        if (p == null) {
            return getRootInode();
        }
        return new HadoopInode(p);
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        HadoopInode hinode = (HadoopInode) inode;
        Path p = _inodeMap.inverse().get(hinode.uuid);

        int n = -1;
        FSDataInputStream in = _inDescriptorCache.getUnchecked(p);
        n = in.read(offset, data, 0, count);

        return n;
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean remove(Inode parent, String path) throws IOException {
        HadoopInode hinode = (HadoopInode) parent;
        Path p = new Path(hinode.getPath(), path);

        return _fs.delete(p, false);
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, int uid, int gid, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int write(Inode inode, byte[] data, long offset, int count) throws IOException {
        HadoopInode hinode = (HadoopInode) inode;
        Path p = _inodeMap.inverse().get(hinode.uuid);
        int n = -1;
        FSDataOutputStream out = _fs.create(p);
        out.write(data, (int) offset, count);
        n = count;
        out.close();
        return n;
    }

    private synchronized UUID idOf(Path p) {
        UUID uuid = _inodeMap.get(p);
        if (uuid == null) {
            uuid = UUID.randomUUID();
            _inodeMap.put(p, uuid);
        }
        return uuid;
    }

    private synchronized Path pathOf(UUID uuid) {
        Path p = _inodeMap.inverse().get(uuid);
        return p;
    }

    private byte[] toInode(UUID uuid) {
        byte[] fh = new byte[16];
        Bytes.putLong(fh, 0, uuid.getMostSignificantBits());
        Bytes.putLong(fh, 8, uuid.getLeastSignificantBits());
        return fh;
    }

    private UUID toUUID(byte[] data) {
        return new UUID(Bytes.getLong(data, 0), Bytes.getLong(data, 8));
    }

    private class HadoopInode implements Inode {

        private final Path p;
        private final UUID uuid;

        public HadoopInode(UUID uuid) {
            this.uuid = uuid;
            this.p = pathOf(uuid);
        }

        public HadoopInode(Path p) {
            this.p = p;
            this.uuid = idOf(p);
        }

        @Override
        public boolean exists() {
            try {
                return _fs.exists(p);
            } catch (IOException e) {
                return false;
            }
        }

        @Override
        public nfsace4[] getAcl() throws IOException {
            return new nfsace4[0];
        }

        @Override
        public long id() {
            return p.hashCode();
        }

        @Override
        public void setATime(long time) throws IOException {
            // nop
        }

        @Override
        public void setAcl(nfsace4[] acl) throws IOException {
            // nop
        }

        @Override
        public void setCTime(long time) throws IOException {
            // nop
        }

        @Override
        public void setGID(int id) throws IOException {
            // nop
        }

        @Override
        public void setMTime(long time) throws IOException {
            // nop
        }

        @Override
        public void setMode(int size) throws IOException {
            // nop
        }

        @Override
        public void setSize(long size) throws IOException {
            // nop
        }

        @Override
        public void setUID(int id) throws IOException {
            //
        }

        @Override
        public Stat stat() throws IOException {

            FileStatus stat = _fs.getFileStatus(p);
            boolean isDir = stat.isDir();

            Stat unixStat = new Stat();
            unixStat.setATime(stat.getAccessTime());
            unixStat.setMTime(stat.getModificationTime());
            unixStat.setSize(stat.getLen());

            unixStat.setMode(0x777 | (isDir ? 0040000 : 0100000));
            return unixStat;
        }

        @Override
        public Stat statCache() throws IOException {
            return stat();
        }

        @Override
        public byte[] toFileHandle() {
            return toInode(this.uuid);
        }

        @Override
        public Type type() {
            try {
                return _fs.isFile(p) ? Type.REGULAR : Type.DIRECTORY;
            } catch (IOException e) {
                return Type.REGULAR;
            }
        }

        Path getPath() {
            return p;
        }
    }

    private class HadoopFileOpener extends CacheLoader<Path, FSDataInputStream> {

        @Override
        public FSDataInputStream load(Path p) throws Exception {
            return _fs.open(p);
        }
    }

    private class HadoopFileCloser implements RemovalListener<Path, FSDataInputStream> {

        @Override
        public void onRemoval(RemovalNotification<Path, FSDataInputStream> rn) {
            try {
                rn.getValue().close();
            } catch (IOException e) {
                // NOP
            }
        }
    }
}
