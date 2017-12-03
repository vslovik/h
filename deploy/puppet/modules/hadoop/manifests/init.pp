class hadoop ($hadoop_security_authentication = "simple",
  # Set from facter if available
  $hadoop_storage_dirs = split($::hadoop_storage_dirs, ";"),
  $generate_secrets = false,
) {

  include stdlib

  class deploy ($roles) {

    if ("datanode" in $roles) {
      include hadoop::datanode
    }

    if ("namenode" in $roles) {
      include hadoop::init_hdfs
      include hadoop::namenode

      if ("datanode" in $roles) {
        Class['Hadoop::Namenode'] -> Class['Hadoop::Datanode'] -> Class['Hadoop::Init_hdfs']
      } else {
        Class['Hadoop::Namenode'] -> Class['Hadoop::Init_hdfs']
      }
    }

    if ("standby-namenode" in $roles and $hadoop::common_hdfs::ha != "disabled") {
      include hadoop::namenode
    }

    if ("mapred-app" in $roles) {
      include hadoop::mapred_app
    }

    if ("nodemanager" in $roles) {
      include hadoop::nodemanager
    }

    if ("resourcemanager" in $roles) {
      include hadoop::resourcemanager
      include hadoop::historyserver
      include hadoop::proxyserver

      if ("nodemanager" in $roles) {
        Class['Hadoop::Resourcemanager'] -> Class['Hadoop::Nodemanager']
      }
    }

    if ("secondarynamenode" in $roles and $hadoop::common_hdfs::ha == "disabled") {
      include hadoop::secondarynamenode
    }

    if ("hadoop-client" in $roles) {
      include hadoop::client
    }
  }

  class init_hdfs {
    exec { "init hdfs":
      path    => ['/bin','/sbin','/usr/bin','/usr/sbin'],
      command => 'bash -x /usr/lib/hadoop/libexec/init-hdfs.sh',
      require => Package['hadoop-hdfs']
    }
  }

  class common ($hadoop_java_home = undef,
    $hadoop_classpath = undef,
    $hadoop_heapsize = undef,
    $hadoop_opts = undef,
    $hadoop_namenode_opts = undef,
    $hadoop_secondarynamenode_opts = undef,
    $hadoop_datanode_opts = undef,
    $hadoop_balancer_opts = undef,
    $hadoop_jobtracker_opts = undef,
    $hadoop_tasktracker_opts = undef,
    $hadoop_client_opts = undef,
    $hadoop_ssh_opts = undef,
    $hadoop_log_dir = undef,
    $hadoop_slaves = undef,
    $hadoop_master = undef,
    $hadoop_slave_sleep = undef,
    $hadoop_pid_dir = undef,
    $hadoop_ident_string = undef,
    $hadoop_niceness = undef,
    $use_tez = false,
    $tez_conf_dir = undef,
    $tez_jars = undef,
  ) inherits hadoop {

    file {
      "/etc/hadoop/conf/hadoop-env.sh":
        content => template('hadoop/hadoop-env.sh'),
        require => [Package["hadoop"]],
    }

    package { "hadoop":
      ensure => latest,
      require => Package["jdk"],
    }
  }

  class common_yarn (
    $yarn_data_dirs = suffix($hadoop::hadoop_storage_dirs, "/yarn"),
    $hadoop_ps_host,
    $hadoop_ps_port = "20888",
    $hadoop_rm_host,
    $hadoop_rm_port = "8032",
    $hadoop_rm_admin_port = "8033",
    $hadoop_rm_webapp_port = "8088",
    $hadoop_rm_bind_host = undef,
    $hadoop_rt_port = "8025",
    $hadoop_sc_port = "8030",
    $yarn_log_server_url = undef,
    $yarn_nodemanager_resource_memory_mb = undef,
    $yarn_scheduler_maximum_allocation_mb = undef,
    $yarn_scheduler_minimum_allocation_mb = undef,
    $yarn_resourcemanager_scheduler_class = undef,
    $yarn_resourcemanager_ha_enabled = undef,
    $yarn_resourcemanager_cluster_id = "ha-rm-uri",
    $yarn_resourcemanager_zk_address = $hadoop::zk,
    # work around https://issues.apache.org/jira/browse/YARN-2847 by default
    $container_executor_banned_users = "doesnotexist",
    $container_executor_min_user_id = "499",
    $hadoop_security_authentication = $hadoop::hadoop_security_authentication,
    $yarn_nodemanager_vmem_check_enabled = undef,
  ) inherits hadoop {

    include hadoop::common

    package { "hadoop-yarn":
      ensure => latest,
      require => [Package["jdk"], Package["hadoop"]],
    }

    file {
      "/etc/hadoop/conf/yarn-site.xml":
        content => template('hadoop/yarn-site.xml'),
        require => [Package["hadoop"]],
    }

    file { "/etc/hadoop/conf/container-executor.cfg":
      content => template('hadoop/container-executor.cfg'),
      require => [Package["hadoop"]],
    }
  }

  class common_hdfs ($ha = "disabled",
    $hadoop_config_dfs_block_size = undef,
    $hadoop_config_namenode_handler_count = undef,
    $hadoop_dfs_datanode_plugins = "",
    $hadoop_dfs_namenode_plugins = "",
    $hadoop_namenode_host = $fqdn,
    $hadoop_namenode_port = "8020",
    $hadoop_namenode_bind_host = undef,
    $hadoop_namenode_http_port = "50070",
    $hadoop_namenode_http_bind_host = undef,
    $hadoop_namenode_https_port = "50470",
    $hadoop_namenode_https_bind_host = undef,
    $hdfs_data_dirs = suffix($hadoop::hadoop_storage_dirs, "/hdfs"),
    $hdfs_shortcut_reader = undef,
    $hdfs_support_append = undef,
    $hdfs_replace_datanode_on_failure = undef,
    $hdfs_webhdfs_enabled = "true",
    $hdfs_replication = undef,
    $hdfs_datanode_fsdataset_volume_choosing_policy = undef,
    $hdfs_nfs_bridge = "disabled",
    $hdfs_nfs_bridge_user = undef,
    $hdfs_nfs_gw_host = undef,
    $hdfs_nfs_proxy_groups = undef,
    $namenode_data_dirs = suffix($hadoop::hadoop_storage_dirs, "/namenode"),
    $nameservice_id = "ha-nn-uri",
    $journalnode_host = "0.0.0.0",
    $journalnode_port = "8485",
    $journalnode_http_port = "8480",
    $journalnode_https_port = "8481",
    $journalnode_edits_dir = "${hadoop::hadoop_storage_dirs[0]}/journalnode",
    $shared_edits_dir = "/hdfs_shared",
    $testonly_hdfs_sshkeys  = "no",
    $hadoop_ha_sshfence_user_home = "/var/lib/hadoop-hdfs",
    $sshfence_privkey = "hadoop/id_sshfence",
    $sshfence_pubkey = "hadoop/id_sshfence.pub",
    $sshfence_user = "hdfs",
    $zk = $hadoop::zk,
    $hadoop_config_fs_inmemory_size_mb = undef,
    $hadoop_security_group_mapping = undef,
    $hadoop_core_proxyusers = $hadoop::proxyusers,
    $hadoop_snappy_codec = undef,
    $hadoop_security_authentication = $hadoop::hadoop_security_authentication,
    $hadoop_http_authentication_type = undef,
    $hadoop_http_authentication_signature_secret = undef,
    $hadoop_http_authentication_signature_secret_file = "/etc/hadoop/conf/hadoop-http-authentication-signature-secret",
    $hadoop_http_authentication_cookie_domain = regsubst($fqdn, "^[^\\.]+\\.", ""),
    $generate_secrets = $hadoop::generate_secrets,
    $namenode_datanode_registration_ip_hostname_check = undef,
  ) inherits hadoop {

    $sshfence_keydir  = "$hadoop_ha_sshfence_user_home/.ssh"
    $sshfence_keypath = "$sshfence_keydir/id_sshfence"

    include hadoop::common

    # Check if test mode is enforced, so we can install hdfs ssh-keys for passwordless
    if ($testonly_hdfs_sshkeys == "yes") {
      notify{"WARNING: provided hdfs ssh keys are for testing purposes only.\n
        They shouldn't be used in production cluster": }
      $ssh_user        = "hdfs"
      $ssh_user_home   = "/var/lib/hadoop-hdfs"
      $ssh_user_keydir = "$ssh_user_home/.ssh"
      $ssh_keypath     = "$ssh_user_keydir/id_hdfsuser"
      $ssh_privkey     = "hadoop/hdfs/id_hdfsuser"
      $ssh_pubkey      = "hadoop/hdfs/id_hdfsuser.pub"

      file { $ssh_user_keydir:
        ensure  => directory,
        owner   => 'hdfs',
        group   => 'hdfs',
        mode    => '0700',
        require => Package["hadoop-hdfs"],
      }

      file { $ssh_keypath:
        source  => "puppet:///modules/$ssh_privkey",
        owner   => 'hdfs',
        group   => 'hdfs',
        mode    => '0600',
        require => File[$ssh_user_keydir],
      }

      file { "$ssh_user_keydir/authorized_keys":
        source  => "puppet:///modules/$ssh_pubkey",
        owner   => 'hdfs',
        group   => 'hdfs',
        mode    => '0600',
        require => File[$ssh_user_keydir],
      }
    }

    package { "hadoop-hdfs":
      ensure => latest,
      require => [Package["jdk"], Package["hadoop"]],
    }

    file {
      "/etc/hadoop/conf/core-site.xml":
        content => template('hadoop/core-site.xml'),
        require => [Package["hadoop"]],
    }

    file {
      "/etc/hadoop/conf/hdfs-site.xml":
        content => template('hadoop/hdfs-site.xml'),
        require => [Package["hadoop"]],
    }
  }

  class common_mapred_app (
    $mapreduce_cluster_acls_enabled = undef,
    $mapreduce_jobhistory_host = undef,
    $mapreduce_jobhistory_port = "10020",
    $mapreduce_jobhistory_webapp_port = "19888",
    $mapreduce_framework_name = "yarn",
    $mapred_data_dirs = suffix($hadoop::hadoop_storage_dirs, "/mapred"),
    $mapreduce_cluster_temp_dir = "/mapred/system",
    $yarn_app_mapreduce_am_staging_dir = "/user",
    $mapreduce_task_io_sort_factor = 64,              # 10 default
    $mapreduce_task_io_sort_mb = 256,                 # 100 default
    $mapreduce_reduce_shuffle_parallelcopies = undef, # 5 is default
    # processorcount == facter fact
    $mapreduce_tasktracker_map_tasks_maximum = inline_template("<%= [1, @processorcount.to_i * 0.20].max.round %>"),
    $mapreduce_tasktracker_reduce_tasks_maximum = inline_template("<%= [1, @processorcount.to_i * 0.20].max.round %>"),
    $mapreduce_tasktracker_http_threads = 60,         # 40 default
    $mapreduce_output_fileoutputformat_compress_type = "BLOCK", # "RECORD" default
    $mapreduce_map_output_compress = undef,
    $mapreduce_job_reduce_slowstart_completedmaps = undef,
    $mapreduce_map_memory_mb = undef,
    $mapreduce_reduce_memory_mb = undef,
    $mapreduce_map_java_opts = "-Xmx1024m",
    $mapreduce_reduce_java_opts = "-Xmx1024m",
    $hadoop_security_authentication = $hadoop::hadoop_security_authentication,
  ) inherits hadoop {
    include hadoop::common_hdfs

    package { "hadoop-mapreduce":
      ensure => latest,
      require => [Package["jdk"], Package["hadoop"]],
    }

    file {
      "/etc/hadoop/conf/mapred-site.xml":
        content => template('hadoop/mapred-site.xml'),
        require => [Package["hadoop"]],
    }

    file { "/etc/hadoop/conf/taskcontroller.cfg":
      content => template('hadoop/taskcontroller.cfg'),
      require => [Package["hadoop"]],
    }
  }

  class datanode (
    $hadoop_security_authentication = $hadoop::hadoop_security_authentication,
  ) inherits hadoop {
    include hadoop::common_hdfs

    package { "hadoop-hdfs-datanode":
      ensure => latest,
      require => Package["jdk"],
    }

    file {
      "/etc/default/hadoop-hdfs-datanode":
        content => template('hadoop/hadoop-hdfs'),
        require => [Package["hadoop-hdfs-datanode"]],
    }

    service { "hadoop-hdfs-datanode":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-hdfs-datanode"], File["/etc/hadoop/conf/core-site.xml"], File["/etc/hadoop/conf/hdfs-site.xml"], File["/etc/hadoop/conf/hadoop-env.sh"]],
      require => [ Package["hadoop-hdfs-datanode"], File["/etc/default/hadoop-hdfs-datanode"], File[$hadoop::common_hdfs::hdfs_data_dirs] ],
    }

    Service<| title == 'hadoop-hdfs-namenode' |> -> Service['hadoop-hdfs-datanode']

    hadoop::create_storage_dir { $hadoop::common_hdfs::hdfs_data_dirs: } ->
    file { $hadoop::common_hdfs::hdfs_data_dirs:
      ensure => directory,
      owner => hdfs,
      group => hdfs,
      mode => '755',
      require => [ Package["hadoop-hdfs"] ],
    }
  }

  class create_hdfs_dirs($hdfs_dirs_meta,
    $hadoop_security_authentcation = $hadoop::hadoop_security_authentication ) inherits hadoop {
    $user = $hdfs_dirs_meta[$title][user]
    $perm = $hdfs_dirs_meta[$title][perm]

    exec { "HDFS init $title":
      user => "hdfs",
      command => "/bin/bash -c 'hadoop fs -mkdir $title ; hadoop fs -chmod $perm $title && hadoop fs -chown $user $title'",
      require => Service["hadoop-hdfs-namenode"],
    }
    Exec <| title == "activate nn1" |>  -> Exec["HDFS init $title"]
  }

  class rsync_hdfs($files,
    $hadoop_security_authentcation = $hadoop::hadoop_security_authentication ) inherits hadoop {
    $src = $files[$title]

    exec { "HDFS rsync $title":
      user => "hdfs",
      command => "/bin/bash -c 'hadoop fs -mkdir -p $title ; hadoop fs -put -f $src $title'",
      require => Service["hadoop-hdfs-namenode"],
    }
    Exec <| title == "activate nn1" |>  -> Exec["HDFS rsync $title"]
  }

  class namenode ( $nfs_server = "", $nfs_path = "",
    $standby_bootstrap_retries = 10,
    # milliseconds
    $standby_bootstrap_retry_interval = 30000) {
    include hadoop::common_hdfs

    if ($hadoop::common_hdfs::ha != 'disabled') {
      file { $hadoop::common_hdfs::sshfence_keydir:
        ensure  => directory,
        owner   => 'hdfs',
        group   => 'hdfs',
        mode    => '0700',
        require => Package["hadoop-hdfs"],
      }

      file { $hadoop::common_hdfs::sshfence_keypath:
        source  => "puppet:///files/$hadoop::common_hdfs::sshfence_privkey",
        owner   => 'hdfs',
        group   => 'hdfs',
        mode    => '0600',
        before  => Service["hadoop-hdfs-namenode"],
        require => File[$hadoop::common_hdfs::sshfence_keydir],
      }

      file { "$hadoop::common_hdfs::sshfence_keydir/authorized_keys":
        source  => "puppet:///files/$hadoop::common_hdfs::sshfence_pubkey",
        owner   => 'hdfs',
        group   => 'hdfs',
        mode    => '0600',
        before  => Service["hadoop-hdfs-namenode"],
        require => File[$hadoop::common_hdfs::sshfence_keydir],
      }

      if (! ('qjournal://' in $hadoop::common_hdfs::shared_edits_dir)) {
        hadoop::create_storage_dir { $hadoop::common_hdfs::shared_edits_dir: } ->
        file { $hadoop::common_hdfs::shared_edits_dir:
          ensure => directory,
        }

        if ($nfs_server) {
          if (!$nfs_path) {
            fail("No nfs share specified for shared edits dir")
          }

          require nfs::client

          mount { $hadoop::common_hdfs::shared_edits_dir:
            ensure  => "mounted",
            atboot  => true,
            device  => "${nfs_server}:${nfs_path}",
            fstype  => "nfs",
            options => "tcp,soft,timeo=10,intr,rsize=32768,wsize=32768",
            require => File[$hadoop::common::hdfs::shared_edits_dir],
            before  => Service["hadoop-hdfs-namenode"],
          }
        }
      }
    }

    package { "hadoop-hdfs-namenode":
      ensure => latest,
      require => Package["jdk"],
    }

    service { "hadoop-hdfs-namenode":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-hdfs-namenode"], File["/etc/hadoop/conf/core-site.xml"], File["/etc/hadoop/conf/hdfs-site.xml"], File["/etc/hadoop/conf/hadoop-env.sh"]],
      require => [Package["hadoop-hdfs-namenode"]],
    }

    if ($hadoop::common_hdfs::ha == "auto") {
      package { "hadoop-hdfs-zkfc":
        ensure => latest,
        require => Package["jdk"],
      }

      service { "hadoop-hdfs-zkfc":
        ensure => running,
        hasstatus => true,
        subscribe => [Package["hadoop-hdfs-zkfc"], File["/etc/hadoop/conf/core-site.xml"], File["/etc/hadoop/conf/hdfs-site.xml"], File["/etc/hadoop/conf/hadoop-env.sh"]],
        require => [Package["hadoop-hdfs-zkfc"]],
      }
      Service <| title == "hadoop-hdfs-zkfc" |> -> Service <| title == "hadoop-hdfs-namenode" |>
    }

    $namenode_array = any2array($hadoop::common_hdfs::hadoop_namenode_host)
    $first_namenode = $namenode_array[0]
    if ($::fqdn == $first_namenode) {
      exec { "namenode format":
        user => "hdfs",
        command => "/bin/bash -c 'hdfs namenode -format -nonInteractive >> /var/lib/hadoop-hdfs/nn.format.log 2>&1'",
        returns => [ 0, 1],
        creates => "${hadoop::common_hdfs::namenode_data_dirs[0]}/current/VERSION",
        require => [ Package["hadoop-hdfs-namenode"], File[$hadoop::common_hdfs::namenode_data_dirs], File["/etc/hadoop/conf/hdfs-site.xml"] ],
        tag     => "namenode-format",
      }

      if ($hadoop::common_hdfs::ha != "disabled") {
        if ($hadoop::common_hdfs::ha == "auto") {
          exec { "namenode zk format":
            user => "hdfs",
            command => "/bin/bash -c 'hdfs zkfc -formatZK -nonInteractive >> /var/lib/hadoop-hdfs/zk.format.log 2>&1'",
            returns => [ 0, 2],
            require => [ Package["hadoop-hdfs-zkfc"], File["/etc/hadoop/conf/hdfs-site.xml"] ],
            tag     => "namenode-format",
          }
          Service <| title == "zookeeper-server" |> -> Exec <| title == "namenode zk format" |>
          Exec <| title == "namenode zk format" |>  -> Service <| title == "hadoop-hdfs-zkfc" |>
        } else {
          exec { "activate nn1":
            command => "/usr/bin/hdfs haadmin -transitionToActive nn1",
            user    => "hdfs",
            unless  => "/usr/bin/test $(/usr/bin/hdfs haadmin -getServiceState nn1) = active",
            require => Service["hadoop-hdfs-namenode"],
          }
        }
      }
    } elsif ($hadoop::common_hdfs::ha == "auto") {
      $retry_params = "-Dipc.client.connect.max.retries=$standby_bootstrap_retries \
        -Dipc.client.connect.retry.interval=$standby_bootstrap_retry_interval"

      exec { "namenode bootstrap standby":
        user => "hdfs",
        # first namenode might be rebooting just now so try for some time
        command => "/bin/bash -c 'hdfs namenode -bootstrapStandby $retry_params >> /var/lib/hadoop-hdfs/nn.bootstrap-standby.log 2>&1'",
        creates => "${hadoop::common_hdfs::namenode_data_dirs[0]}/current/VERSION",
        require => [ Package["hadoop-hdfs-namenode"], File[$hadoop::common_hdfs::namenode_data_dirs], File["/etc/hadoop/conf/hdfs-site.xml"] ],
        tag     => "namenode-format",
      }
    } elsif ($hadoop::common_hdfs::ha != "disabled") {
      hadoop::namedir_copy { $hadoop::common_hdfs::namenode_data_dirs:
        source       => $first_namenode,
        ssh_identity => $hadoop::common_hdfs::sshfence_keypath,
        require      => File[$hadoop::common_hdfs::sshfence_keypath],
      }
    }

    file {
      "/etc/default/hadoop-hdfs-namenode":
        content => template('hadoop/hadoop-hdfs'),
        require => [Package["hadoop-hdfs-namenode"]],
    }

    hadoop::create_storage_dir { $hadoop::common_hdfs::namenode_data_dirs: } ->
    file { $hadoop::common_hdfs::namenode_data_dirs:
      ensure => directory,
      owner => hdfs,
      group => hdfs,
      mode => '700',
      require => [Package["hadoop-hdfs"]],
    }
  }

  define create_storage_dir {
    exec { "mkdir $name":
      command => "/bin/mkdir -p $name",
      creates => $name,
      user =>"root",
    }
  }

  define namedir_copy ($source, $ssh_identity) {
    exec { "copy namedir $title from first namenode":
      command => "/usr/bin/rsync -avz -e '/usr/bin/ssh -oStrictHostKeyChecking=no -i $ssh_identity' '${source}:$title/' '$title/'",
      user    => "hdfs",
      tag     => "namenode-format",
      creates => "$title/current/VERSION",
    }
  }

  class secondarynamenode {
    include hadoop::common_hdfs

    package { "hadoop-hdfs-secondarynamenode":
      ensure => latest,
      require => Package["jdk"],
    }

    file {
      "/etc/default/hadoop-hdfs-secondarynamenode":
        content => template('hadoop/hadoop-hdfs'),
        require => [Package["hadoop-hdfs-secondarynamenode"]],
    }

    service { "hadoop-hdfs-secondarynamenode":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-hdfs-secondarynamenode"], File["/etc/hadoop/conf/core-site.xml"], File["/etc/hadoop/conf/hdfs-site.xml"], File["/etc/hadoop/conf/hadoop-env.sh"]],
      require => [Package["hadoop-hdfs-secondarynamenode"]],
    }

  }

  class journalnode {
    include hadoop::common_hdfs

    package { "hadoop-hdfs-journalnode":
      ensure => latest,
      require => Package["jdk"],
    }

    $journalnode_cluster_journal_dir = "${hadoop::common_hdfs::journalnode_edits_dir}/${hadoop::common_hdfs::nameservice_id}"

    service { "hadoop-hdfs-journalnode":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-hdfs-journalnode"], File["/etc/hadoop/conf/hadoop-env.sh"],
        File["/etc/hadoop/conf/hdfs-site.xml"], File["/etc/hadoop/conf/core-site.xml"]],
      require => [ Package["hadoop-hdfs-journalnode"], File[$journalnode_cluster_journal_dir] ],
    }

    hadoop::create_storage_dir { [$hadoop::common_hdfs::journalnode_edits_dir, $journalnode_cluster_journal_dir]: } ->
    file { [ "${hadoop::common_hdfs::journalnode_edits_dir}", "$journalnode_cluster_journal_dir" ]:
      ensure => directory,
      owner => 'hdfs',
      group => 'hdfs',
      mode => '755',
      require => [Package["hadoop-hdfs"]],
    }
  }


  class resourcemanager {
    include hadoop::common_yarn

    package { "hadoop-yarn-resourcemanager":
      ensure => latest,
      require => Package["jdk"],
    }

    service { "hadoop-yarn-resourcemanager":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-yarn-resourcemanager"], File["/etc/hadoop/conf/hadoop-env.sh"],
        File["/etc/hadoop/conf/yarn-site.xml"], File["/etc/hadoop/conf/core-site.xml"]],
      require => [ Package["hadoop-yarn-resourcemanager"] ],
    }

  }

  class proxyserver {
    include hadoop::common_yarn

    package { "hadoop-yarn-proxyserver":
      ensure => latest,
      require => Package["jdk"],
    }

    service { "hadoop-yarn-proxyserver":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-yarn-proxyserver"], File["/etc/hadoop/conf/hadoop-env.sh"],
        File["/etc/hadoop/conf/yarn-site.xml"], File["/etc/hadoop/conf/core-site.xml"]],
      require => [ Package["hadoop-yarn-proxyserver"] ],
    }
  }

  class historyserver {
    include hadoop::common_mapred_app

    package { "hadoop-mapreduce-historyserver":
      ensure => latest,
      require => Package["jdk"],
    }

    service { "hadoop-mapreduce-historyserver":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-mapreduce-historyserver"], File["/etc/hadoop/conf/hadoop-env.sh"],
        File["/etc/hadoop/conf/yarn-site.xml"], File["/etc/hadoop/conf/core-site.xml"]],
      require => [Package["hadoop-mapreduce-historyserver"]],
    }
  }

  class nodemanager {
    include hadoop::common_mapred_app
    include hadoop::common_yarn

    package { "hadoop-yarn-nodemanager":
      ensure => latest,
      require => Package["jdk"],
    }

    service { "hadoop-yarn-nodemanager":
      ensure => running,
      hasstatus => true,
      subscribe => [Package["hadoop-yarn-nodemanager"], File["/etc/hadoop/conf/hadoop-env.sh"],
        File["/etc/hadoop/conf/yarn-site.xml"], File["/etc/hadoop/conf/core-site.xml"]],
      require => [ Package["hadoop-yarn-nodemanager"], File[$hadoop::common_yarn::yarn_data_dirs] ],
    }

    hadoop::create_storage_dir { $hadoop::common_yarn::yarn_data_dirs: } ->
    file { $hadoop::common_yarn::yarn_data_dirs:
      ensure => directory,
      owner => yarn,
      group => yarn,
      mode => '755',
      require => [Package["hadoop-yarn"]],
    }
  }

  class mapred_app {
    include hadoop::common_mapred_app

    hadoop::create_storage_dir { $hadoop::common_mapred_app::mapred_data_dirs: } ->
    file { $hadoop::common_mapred_app::mapred_data_dirs:
      ensure => directory,
      owner => yarn,
      group => yarn,
      mode => '755',
      require => [Package["hadoop-mapreduce"]],
    }
  }

  class client {
    include hadoop::common_mapred_app
    include hadoop::common_yarn

    $hadoop_client_packages = $operatingsystem ? {
      /(OracleLinux|CentOS|RedHat|Fedora)/  => [ "hadoop-doc", "hadoop-hdfs-fuse", "hadoop-client", "hadoop-libhdfs", "hadoop-debuginfo" ],
      /(SLES|OpenSuSE)/                     => [ "hadoop-doc", "hadoop-hdfs-fuse", "hadoop-client", "hadoop-libhdfs" ],
      /(Ubuntu|Debian)/                     => [ "hadoop-doc", "hadoop-hdfs-fuse", "hadoop-client", "libhdfs0-dev"   ],
      default                               => [ "hadoop-doc", "hadoop-hdfs-fuse", "hadoop-client" ],
    }

    package { $hadoop_client_packages:
      ensure => latest,
      require => [Package["jdk"], Package["hadoop"], Package["hadoop-hdfs"], Package["hadoop-mapreduce"]],
    }
  }
}
