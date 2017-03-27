/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemFactoryDelegate;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;
import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT;

/**
 * Tests secondary file system configuration.
 */
public class HadoopSecondaryFileSystemConfigurationTest extends IgfsCommonAbstractTest {
    /** IGFS scheme */
    static final String IGFS_SCHEME = "igfs";

    /** Primary file system authority. */
    private static final String PRIMARY_AUTHORITY = "igfs@";

    /** Autogenerated secondary file system configuration path. */
    private static final String PRIMARY_CFG_PATH = "/work/core-site-primary-test.xml";

    /** Secondary file system authority. */
    private static final String SECONDARY_AUTHORITY = "igfs_secondary@127.0.0.1:11500";

    /** Autogenerated secondary file system configuration path. */
    static final String SECONDARY_CFG_PATH = "/work/core-site-test.xml";

    /** Secondary endpoint configuration. */
    protected static final IgfsIpcEndpointConfiguration SECONDARY_ENDPOINT_CFG;

    /** Group size. */
    public static final int GRP_SIZE = 128;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Primary file system URI. */
    protected URI primaryFsUri;

    /** Primary file system. */
    private FileSystem primaryFs;

    /** Full path of primary Fs configuration */
    private String primaryConfFullPath;

    /** Input primary Fs uri */
    private String primaryFsUriStr;

    /** Input URI scheme for configuration */
    private String primaryCfgScheme;

    /** Input URI authority for configuration */
    private String primaryCfgAuthority;

    /** if to pass configuration */
    private boolean passPrimaryConfiguration;

    /** Full path of s Fs configuration */
    private String secondaryConfFullPath;

    /** /Input URI scheme for configuration */
    private String secondaryFsUriStr;

    /** Input URI scheme for configuration */
    private String secondaryCfgScheme;

    /** Input URI authority for configuration */
    private String secondaryCfgAuthority;

    /** if to pass configuration */
    private boolean passSecondaryConfiguration;

    /** Default IGFS mode. */
    protected final IgfsMode mode;

    /** Skip embedded mode flag. */
    private final boolean skipEmbed;

    /** Skip local shmem flag. */
    private final boolean skipLocShmem;

    static {
        SECONDARY_ENDPOINT_CFG = new IgfsIpcEndpointConfiguration();

        SECONDARY_ENDPOINT_CFG.setType(IgfsIpcEndpointType.TCP);
        SECONDARY_ENDPOINT_CFG.setPort(11500);
    }

    /**
     * Constructor.
     *
     * @param mode Default IGFS mode.
     * @param skipEmbed Whether to skip embedded mode.
     * @param skipLocShmem Whether to skip local shmem mode.
     */
    protected HadoopSecondaryFileSystemConfigurationTest(IgfsMode mode, boolean skipEmbed, boolean skipLocShmem) {
        this.mode = mode;
        this.skipEmbed = skipEmbed;
        this.skipLocShmem = skipLocShmem;
    }

    /**
     * Default constructor.
     */
    public HadoopSecondaryFileSystemConfigurationTest() {
        this(PROXY, true, false);
    }

    /**
     * Executes before each test.
     * @throws Exception If failed.
     */
    private void before() throws Exception {
        initSecondary();

        if (passPrimaryConfiguration) {
            Configuration primaryFsCfg = configuration(primaryCfgScheme, primaryCfgAuthority, skipEmbed, skipLocShmem);

            primaryConfFullPath = writeConfiguration(primaryFsCfg, PRIMARY_CFG_PATH);
        }
        else
            primaryConfFullPath = null;

        CachingHadoopFileSystemFactory fac = new CachingHadoopFileSystemFactory();

        fac.setConfigPaths(primaryConfFullPath);
        fac.setUri(primaryFsUriStr);

        HadoopFileSystemFactoryDelegate facDelegate = HadoopDelegateUtils.fileSystemFactoryDelegate(
            getClass().getClassLoader(), fac);

        facDelegate.start();

        primaryFs = (FileSystem)facDelegate.get(null); //provider.createFileSystem(null);

        primaryFsUri = primaryFs.getUri();
    }

    /**
     * Executes after each test.
     * @throws Exception If failed.
     */
    private void after() throws Exception {
        if (primaryFs != null) {
            try {
                primaryFs.delete(new Path("/"), true);
            }
            catch (Exception ignore) {
                // No-op.
            }

            U.closeQuiet(primaryFs);
        }

        G.stopAll(true);

        delete(primaryConfFullPath);
        delete(secondaryConfFullPath);
    }

    /**
     * Utility method to delete file.
     *
     * @param file the file path to delete.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void delete(String file) {
        if (file != null) {
            new File(file).delete();

            assertFalse(new File(file).exists());
        }
    }

    /**
     * Initialize underlying secondary filesystem.
     *
     * @throws Exception If failed.
     */
    private void initSecondary() throws Exception {
        if (passSecondaryConfiguration) {
            Configuration secondaryConf = configuration(secondaryCfgScheme, secondaryCfgAuthority, true, true);

            secondaryConf.setInt("fs.igfs.block.size", 1024);

            secondaryConfFullPath = writeConfiguration(secondaryConf, SECONDARY_CFG_PATH);
        }
        else
            secondaryConfFullPath = null;

        startNodes();
    }

    /**
     * Starts the nodes for this test.
     *
     * @throws Exception If failed.
     */
    private void startNodes() throws Exception {
        if (mode != PRIMARY)
            startSecondary();

        startGrids(4);
    }

    /**
     * Starts secondary IGFS
     */
    private void startSecondary() {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName("igfs_secondary");
        igfsCfg.setIpcEndpointConfiguration(SECONDARY_ENDPOINT_CFG);
        igfsCfg.setBlockSize(512 * 1024);
        igfsCfg.setPrefetchBlocks(1);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(GRP_SIZE));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        igfsCfg.setDataCacheConfiguration(dataCacheCfg);
        igfsCfg.setMetaCacheConfiguration(metaCacheCfg);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("grid_secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setFileSystemConfiguration(igfsCfg);
        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cfg.setCommunicationSpi(communicationSpi());

        G.start(cfg);
    }

    /**
     * Get primary IPC endpoint configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return IPC primary endpoint configuration.
     */
    protected IgfsIpcEndpointConfiguration primaryIpcEndpointConfiguration(final String igniteInstanceName) {
        IgfsIpcEndpointConfiguration cfg = new IgfsIpcEndpointConfiguration();

        cfg.setType(IgfsIpcEndpointType.TCP);
        cfg.setPort(DFLT_IPC_PORT + getTestIgniteInstanceIndex(igniteInstanceName));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "grid";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setFileSystemConfiguration(fsConfiguration(igniteInstanceName));
        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);
        cfg.setCommunicationSpi(communicationSpi());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Meta cache configuration.
     */
    protected CacheConfiguration metaCacheConfiguration() {

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName("replicated");
        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        return ccfg;
    }

    /**
     * @return Data cache configuration.
     */
    protected CacheConfiguration dataCacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName("partitioned");
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setNearConfiguration(null);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(GRP_SIZE));
        ccfg.setBackups(0);
        ccfg.setAtomicityMode(TRANSACTIONAL);


        return ccfg;
    }

    /**
     * Gets IGFS configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return IGFS configuration.
     */
    protected FileSystemConfiguration fsConfiguration(String igniteInstanceName) throws IgniteCheckedException {
        FileSystemConfiguration cfg = new FileSystemConfiguration();

        cfg.setName("igfs");
        cfg.setPrefetchBlocks(1);
        cfg.setDefaultMode(mode);

        if (mode != PRIMARY)
            cfg.setSecondaryFileSystem(
                new IgniteHadoopIgfsSecondaryFileSystem(secondaryFsUriStr, secondaryConfFullPath));

        cfg.setIpcEndpointConfiguration(primaryIpcEndpointConfiguration(igniteInstanceName));

        cfg.setManagementPort(-1);
        cfg.setBlockSize(512 * 1024); // Together with group blocks mapper will yield 64M per node groups.

        cfg.setDataCacheConfiguration(dataCacheConfiguration());
        cfg.setMetaCacheConfiguration(metaCacheConfiguration());

        return cfg;
    }

    /** @return Communication SPI. */
    private CommunicationSpi communicationSpi() {
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        return commSpi;
    }

    /**
     * Case #SecondaryFileSystemProvider(null, path)
     *
     * @throws Exception On failure.
     */
    public void testFsConfigurationOnly() throws Exception {
        primaryCfgScheme = IGFS_SCHEME;
        primaryCfgAuthority = PRIMARY_AUTHORITY;
        passPrimaryConfiguration = true;
        primaryFsUriStr = null;

        // wrong secondary URI in the configuration:
        secondaryCfgScheme = IGFS_SCHEME;
        secondaryCfgAuthority = SECONDARY_AUTHORITY;
        passSecondaryConfiguration = true;
        secondaryFsUriStr = null;

        check();
    }

    /**
     * Case #SecondaryFileSystemProvider(uri, path), when 'uri' parameter overrides
     * the Fs uri set in the configuration.
     *
     * @throws Exception On failure.
     */
    public void testFsUriOverridesUriInConfiguration() throws Exception {
        // wrong primary URI in the configuration:
        primaryCfgScheme = "foo";
        primaryCfgAuthority = "moo:zoo@bee";
        passPrimaryConfiguration = true;
        primaryFsUriStr = mkUri(IGFS_SCHEME, PRIMARY_AUTHORITY);

        // wrong secondary URI in the configuration:
        secondaryCfgScheme = "foo";
        secondaryCfgAuthority = "moo:zoo@bee";
        passSecondaryConfiguration = true;
        secondaryFsUriStr = mkUri(IGFS_SCHEME, SECONDARY_AUTHORITY);

        check();
    }

    /**
     * Perform actual check.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    private void check() throws Exception {
        before();

        try {
            Path fsHome = new Path(primaryFsUri);
            Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");
            Path file = new Path(dir, "someFile");

            assertPathDoesNotExist(primaryFs, file);

            FsPermission fsPerm = new FsPermission((short)644);

            FSDataOutputStream os = primaryFs.create(file, fsPerm, false, 1, (short)1, 1L, null);

            // Try to write something in file.
            os.write("abc".getBytes());

            os.close();

            // Check file status.
            FileStatus fileStatus = primaryFs.getFileStatus(file);

            assertFalse(fileStatus.isDir());
            assertEquals(file, fileStatus.getPath());
            assertEquals(fsPerm, fileStatus.getPermission());
        }
        finally {
            after();
        }
    }

    /**
     * Create configuration for test.
     *
     * @param skipEmbed Whether to skip embedded mode.
     * @param skipLocShmem Whether to skip local shmem mode.
     * @return Configuration.
     */
    static Configuration configuration(String scheme, String authority, boolean skipEmbed, boolean skipLocShmem) {
        final Configuration cfg = new Configuration();

        if (scheme != null && authority != null)
            cfg.set("fs.defaultFS", scheme + "://" + authority + "/");

        setImplClasses(cfg);

        if (authority != null) {
            if (skipEmbed)
                cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_EMBED, authority), true);

            if (skipLocShmem)
                cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, authority), true);
        }

        return cfg;
    }

    /**
     * Sets Hadoop Fs implementation classes.
     *
     * @param cfg the configuration to set parameters into.
     */
    static void setImplClasses(Configuration cfg) {
        cfg.set("fs.igfs.impl", IgniteHadoopFileSystem.class.getName());

        cfg.set("fs.AbstractFileSystem.igfs.impl",
            org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem.class.getName());
    }

    /**
     * Check path does not exist in a given FileSystem.
     *
     * @param fs FileSystem to check.
     * @param path Path to check.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertPathDoesNotExist(final FileSystem fs, final Path path) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(path);
            }
        }, FileNotFoundException.class, null);
    }

    /**
     * Writes down the configuration to local disk and returns its path.
     *
     * @param cfg the configuration to write.
     * @param pathFromIgniteHome path relatively to Ignite home.
     * @return Full path of the written configuration.
     */
    static String writeConfiguration(Configuration cfg, String pathFromIgniteHome) throws IOException {
        if (!pathFromIgniteHome.startsWith("/"))
            pathFromIgniteHome = "/" + pathFromIgniteHome;

        final String path = U.getIgniteHome() + pathFromIgniteHome;

        delete(path);

        File file = new File(path);

        try (FileOutputStream fos = new FileOutputStream(file)) {
            cfg.writeXml(fos);
        }

        assertTrue(file.exists());
        return path;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60 * 1000;
    }

    /**
     * Makes URI.
     *
     * @param scheme the scheme
     * @param authority the authority
     * @return URI String
     */
    static String mkUri(String scheme, String authority) {
        return scheme + "://" + authority + "/";
    }

    /**
     * Makes URI.
     *
     * @param scheme the scheme
     * @param authority the authority
     * @param path Path part of URI.
     * @return URI String
     */
    static String mkUri(String scheme, String authority, String path) {
        return scheme + "://" + authority + "/" + path;
    }
}