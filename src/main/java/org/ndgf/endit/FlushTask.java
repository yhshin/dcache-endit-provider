/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2015 Gerd Behrmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ndgf.endit;

import diskCacheV111.util.PnfsId;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.nearline.spi.FlushRequest;

import java.nio.file.Paths;
import com.google.common.base.Charsets;

import diskCacheV111.vehicles.StorageInfos;
import org.dcache.vehicles.FileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;

class FlushTask implements PollingTask<Set<URI>>
{
    private final Path outFile;
    private final File file;
    private final PnfsId pnfsId;
    private final String type;
    private final String name;

    private static final Logger LOGGER = LoggerFactory.getLogger(FlushTask.class);

    private final Path errorFile;
    private final Path requestFile;
    private final FileAttributes fileAttributes;
 
    public FlushTask(FlushRequest request, Path outDir, Path reqDir, String type, String name)
    {
        this.type = type;
        this.name = name;
        file = request.getFile();  // deprecated
        //file = Paths.get(request.getReplicaUri());  // dCache v3.0
        outFile = outDir.resolve(file.getName());
        pnfsId = request.getFileAttributes().getPnfsId();
        fileAttributes = request.getFileAttributes();
        errorFile = outDir.resolve(file.getName() + ".err");
        requestFile = reqDir.resolve(file.getName());
    }

    public List<Path> getFilesToWatch()
    {
        return asList(outFile, errorFile);
    }

    @Override
    public Set<URI> start() throws IOException
    {
        LOGGER.warn("FlushTask.start: {}", pnfsId.toString());
        String si = StorageInfos.extractFrom(fileAttributes).toString();
        Files.write(requestFile, si.getBytes(Charsets.UTF_8));
        return null;
    }

    // new to be used with activeWithPath() to get pnfspath
    @Override
    public Set<URI> start(String path) throws IOException
    {
        LOGGER.warn("FlushTask.start: {}", pnfsId.toString());
        String si = StorageInfos.extractFrom(fileAttributes).toString();
        if ( ! si.contains("path=") ) {
            if (path != null && !path.isEmpty()) {
                StringBuffer sb = new StringBuffer("path="+path+";");
                sb.append(si);
                si = sb.toString();
            }
        }
        Files.write(requestFile, si.getBytes(Charsets.UTF_8));
        return null;
    }

    @Override
    public Set<URI> poll() throws URISyntaxException, IOException, InterruptedException, EnditException
    {
        if (Files.exists(errorFile)) {
            List<String> lines;
            try {
                lines = Files.readAllLines(errorFile, Charsets.UTF_8);
            } finally {
                Files.deleteIfExists(outFile);
                Files.deleteIfExists(errorFile);
                Files.deleteIfExists(requestFile);
            }
            LOGGER.info("FlushTask.poll: request failed: {} {}", pnfsId.toString(), lines);
            throw EnditException.create(lines);
        }
        if (Files.exists(outFile)) {
           URI uri = new URI(type, name, null, "bfid=" + pnfsId.toString(), null);
           // URI format: hsmType://hsmInstance/?store=storename&group=groupname&bfid=bfid  
           // <hsmType>: The type of the Tertiary Storage System  
           // <hsmInstance>: The name of the instance  
           // <storename> and <groupname> : The store and group name of the file as provided by the arguments to this executable.  
           // <bfid>: The unique identifier needed to restore or remove the file if necessary.   
           LOGGER.debug("Send back uri: " + uri.toString());
           LOGGER.warn("FlushTask.poll: file on-tape: {}", pnfsId.toString());
           Files.deleteIfExists(outFile);
           Files.deleteIfExists(requestFile);
           return Collections.singleton(uri);
        }
        return null;
    }

    @Override
    public boolean abort() throws IOException
    {
        //return Files.deleteIfExists(outFile);
        LOGGER.info("FlushTask.abort: {}", pnfsId.toString());  
        if (Files.deleteIfExists(requestFile)) {
            Files.deleteIfExists(errorFile);
            Files.deleteIfExists(outFile);
            return true;
        }
        return false;
    }
}
