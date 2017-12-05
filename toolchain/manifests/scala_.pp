class toolchain::scala {
  case $::operatingsystem {
    /Debian/: {
      require apt
      unless $operatingsystemmajrelease > "8" {
         # we pin openjdk-8-* and ca-certificates-java to backports
         require apt::backports

         apt::pin { 'backports_jdk':
            packages => 'openjdk-8-*',
            priority => 500,
            release  => 'jessie-backports',
         } ->
         apt::pin { 'backports_ca':
            packages => 'ca-certificates-java',
            priority => 500,
            release  => 'jessie-backports',
         } ->
         exec {'own_update':
            command => '/usr/bin/apt-get update'
         } -> Package['openjdk-8-jdk']
      }

      package { 'openjdk-8-jdk' :
        ensure => present,
      }
    }
    /Ubuntu/: {
      include apt

      package { 'openjdk-8-jdk' :
        ensure  => present,
        # needed for 14.04 
        require => [ Apt::Ppa[ 'http://ppa.launchpad.net/openjdk-r/ppa/ubuntu'], Class['apt::update'] ]
      }

      apt::key { 'openjdk-ppa':
        id     => 'eb9b1d8886f44e2a',
        server => 'keyserver.ubuntu.com'
      }  ->
      apt::ppa { 'http://ppa.launchpad.net/openjdk-r/ppa/ubuntu':  }
    }
    /(CentOS|Amazon|Fedora)/: {
      package { 'java-1.8.0-openjdk-devel' :
        ensure => present
      }
      if ($::operatingsystem == "Fedora") {
        file { '/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/security/cacerts':
          ensure => 'link',
          target => '/etc/pki/java/cacerts'
        }
      }
    }
    /OpenSuSE/: {
      package { 'java-1_8_0-openjdk-devel' :
        ensure => present
      }
    }
  }
}
