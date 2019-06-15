/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2014-2015 Gerd Behrmann
 *
 * Modifications Copyright (C) 2018 Vincent Garonne
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

import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static java.util.Arrays.asList;

class StageTask implements PollingTask<Set<Checksum>>
{
    private final static Logger LOGGER = LoggerFactory.getLogger(StageTask.class);
    
    private final Path file;
    private final Path inFile;
    private final Path errorFile;
    private final Path requestFile;
    private final long size;
    
    private final String pnfsid;

    StageTask(StageRequest request, Path requestDir, Path inDir)
    {
        file = request.getFile().toPath();  // deprecated
        //file = Paths.get(request.getReplicaUri());  // dCache v3.0
        FileAttributes fileAttributes = request.getFileAttributes();
        pnfsid = fileAttributes.getPnfsId().toString();
        size = fileAttributes.getSize();
        inFile = inDir.resolve(pnfsid);
        errorFile = inDir.resolve(pnfsid + ".err");
        requestFile = requestDir.resolve(pnfsid);
    }

    @Override
    public List<Path> getFilesToWatch()
    {
        return asList(errorFile, inFile);
    }

    @Override
    public Set<Checksum> start() throws Exception
    {
        if (Files.isRegularFile(inFile) && Files.size(inFile) == size) {
            Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
            LOGGER.warn("StageTask.start: found inFile and moved it to pool: {}", pnfsid);
            return Collections.emptySet();
        }
        String s = String.format("size=%d", size);
        Files.write(requestFile, s.getBytes(Charsets.UTF_8));
        return null;
    }

    @Override
    public Set<Checksum> start(String ignored) throws Exception
    {
        return start();
    }

    @Override
    public Set<Checksum> poll() throws IOException, InterruptedException, EnditException
    {
        if (Files.exists(errorFile)) {
            List<String> lines;
            try {
                lines = Files.readAllLines(errorFile, Charsets.UTF_8);
            } finally {
                Files.deleteIfExists(inFile);
                Files.deleteIfExists(errorFile);
                Files.deleteIfExists(requestFile);
            }
            LOGGER.warn("StageTask.poll: request failed: {} {}", pnfsid, lines);
            throw EnditException.create(lines);
        }
        if (Files.isRegularFile(inFile) && Files.size(inFile) == size) {            
            Files.deleteIfExists(requestFile);
            //try {
            Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
            LOGGER.warn("StageTask.poll: moved inFile to pool: {}", pnfsid);
            //} catch (IOException e) {
            //    System.err.println(e);
            //}
            return Collections.emptySet();
        }
        return null;
    }

    @Override
    public boolean abort() throws Exception
    {
        LOGGER.warn("StageTask.abort: {}", pnfsid);
        if (Files.deleteIfExists(requestFile)) {
            Files.deleteIfExists(errorFile);
            Files.deleteIfExists(inFile);
            return true;
        }
        return false;
    }
}
