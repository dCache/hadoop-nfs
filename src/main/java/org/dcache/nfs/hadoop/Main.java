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

import java.io.File;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v3.NfsServerV3;
import org.dcache.chimera.nfs.vfs.VirtualFileSystem;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;

/**
 *
 */
public class Main {
    public static void main(String[] args) throws Exception {
        
        
        VirtualFileSystem vfs = new HadoopVfs("hdfs://localhost:9000");
        OncRpcSvc nfsSvc = new OncRpcSvcBuilder()
                .withPort(2049)
                .withTCP()
                .withAutoPublish()
                .build();
        
        ExportFile exportFile = new ExportFile( new File("/etc/exports"));
        NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
        MountServer mountd = new MountServer(exportFile, vfs);
        
        nfsSvc.register( new OncRpcProgram(100003, 3), nfs3);
        nfsSvc.register( new OncRpcProgram(100005, 3), mountd);
        nfsSvc.start();
        
        System.in.read();
    }
}
