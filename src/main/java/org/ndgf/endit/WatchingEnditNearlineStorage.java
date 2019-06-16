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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Variant of the Endit nearline storage using a WatchService.
 */
public class WatchingEnditNearlineStorage extends AbstractEnditNearlineStorage
{
    private final static Logger LOGGER = LoggerFactory.getLogger(WatchingEnditNearlineStorage.class);

    private final ConcurrentMap<Path,TaskFuture<?>> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private Future<?> watchTask;

    private Future<?> poller;
    private final BlockingQueue<Path> eventQueue = new LinkedBlockingQueue<>();

    public WatchingEnditNearlineStorage(String type, String name)
    {
        super(type, name);
    }

    @Override
    public synchronized void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        super.configure(properties);
        if (watchTask != null) {
            watchTask.cancel(true);
            watchTask = executor.submit(new WatchTask());
        }
        if (poller != null) {
            poller.cancel(true);
            poller = executor.submit(new WatchEventHandler());
        }
    }

    @Override
    protected ListeningExecutorService executor()
    {
        return executor;
    }

    @Override
    protected <T> ListenableFuture<T> schedule(PollingTask<T> task)
    {
        start();
        return new TaskFuture<>(task);
    }

    private synchronized void start()
    {
        if (watchTask == null) {
            watchTask = executor.submit(new WatchTask());
        }
        if (poller == null) {
            poller = executor.submit(new WatchEventHandler());
        }
    }

    @Override
    public synchronized void shutdown()
    {
        if (watchTask != null) {
            watchTask.cancel(true);
        }
        if (poller != null) {
            poller.cancel(true);
        }
        executor.shutdown();
    }

    private class WatchTask implements Runnable
    {
        @Override
        public void run()
        {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
                outDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
                inDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
                //requestDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
                //flushDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);

                pollAll();

                while (!Thread.currentThread().isInterrupted()) {
                    WatchKey key = watcher.take();
                    Path dir = (Path) key.watchable();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.kind().equals(StandardWatchEventKinds.OVERFLOW)) {
                            //pollAll();
                            LOGGER.warn("WatchTask OVFL rcvd: {} {}", dir.getFileName(), event.count());
                            long startTime = System.currentTimeMillis();
                            pollAllChrono();
                            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
                            LOGGER.warn("WatchTask OVFL took {}s", elapsed); 
                        } else {
                            Path fileName = (Path) event.context();
                            poll(dir.resolve(fileName));
                        }
                    }
                    if (!key.reset()) {
                        // TODO
                    }
                }
            } catch (InterruptedException ignored) {
            } catch (IOException e) {
                LOGGER.warn("I/O error while watching Endit directories: {}", e.toString());
            } finally {
                eventQueue.clear();
                for (TaskFuture<?> task : tasks.values()) {
                    task.cancel(true);
                }
            }
        }

        private void poll(Path path)
        {
            if (! eventQueue.contains(path)) {
                try {
                    eventQueue.put(path);
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    LOGGER.error("WatchTask.put.Exception: {} {}", path.getFileName(), e);
                }
            }
        }

        private void pollAll()
        {
            for (TaskFuture<?> task : tasks.values()) {
                task.poll();
            }
        }

        private void pollAllChrono()
        {
            try {
                pollDir(outDir);
                pollDir(inDir);
            }
            catch (Exception e) {
                LOGGER.warn("WT.pollAllChrono: caught exception: {}\n  Fallback to pollAll", e.toString());
                pollAll();  // fallback to original pollAll
            }
        }

        private void pollDir(Path dir) throws IOException 
        {
            File[] files = dir.toFile().listFiles();
            Arrays.sort(files, Comparator.comparingLong(File::lastModified));
            for (File file: files) {
                poll(file.toPath());
            }
        }
    }

    /**
     * Represents the future result of a PollingTask.
     *
     * Periodically polls the task to check whether it has completed. If this Future
     * is cancelled, the task is aborted.
     *
     * @param <V> The result type returned by this Future's <tt>get</tt> method
     */
    private class TaskFuture<V> extends AbstractFuture<V>
    {
        private final PollingTask<V> task;

        TaskFuture(PollingTask<V> task)
        {
            this.task = task;
            register();
        }

        private void register()
        {
            for (Path path : task.getFilesToWatch()) {
                if (tasks.putIfAbsent(path, this) != null) {
                    setException(new IllegalStateException("Duplicate nearline requests on " + path));
                }
            }
        }

        private void unregister()
        {
            for (Path path : task.getFilesToWatch()) {
                tasks.remove(path, this);
                eventQueue.remove(path);
            }
        }

        public synchronized void poll()
        {
            try {
                if (!isDone()) {
                    V result = task.poll();
                    if (result != null) {
                        unregister();
                        set(result);
                    }
                }
            } catch (Exception e) {
                try {
                    task.abort();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
                unregister();
                setException(e);
            }
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning)
        {
            if (isDone()) {
                return false;
            }
            try {
                if (!task.abort()) {
                    return false;
                }
                super.cancel(mayInterruptIfRunning);
            } catch (Exception e) {
                setException(e);
            }
            unregister();
            return true;
        }
    }

    private class WatchEventHandler implements Runnable
    {
        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Path path = eventQueue.take();
                    LOGGER.info("EH: {}", path.getFileName());
                    poll(path);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                LOGGER.error("EH: cought exception: {}", e);
            } finally {
                LOGGER.warn("EH: Finishing", name);
                eventQueue.clear();
            }
        }

        // copy of WatchTask.poll
        private void poll(Path path)
        {
            TaskFuture<?> task = tasks.get(path);  // ConcuttentHashMap is thread-safe
            if (task != null) {
                task.poll();
            }
            else {
                LOGGER.warn("WT.poll: Can't find the task for {}", path.toString());
            }
        }
    }
}
