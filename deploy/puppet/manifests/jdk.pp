$jdk_preinstalled = hiera("matrixnorm::jdk_preinstalled", false)

class jdk {
  case $::operatingsystem {
    /Debian/: {
      require apt
      unless $operatingsystemmajrelease > "8" {
        # we pin openjdk-8-* and ca-certificates-java to backports
        require apt::backports

        Exec['matrixnorm-apt-update'] ->
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
        } -> Package['jdk']
      }
      package { 'jdk':
        name => 'openjdk-8-jdk',
        ensure => present,
        noop => $jdk_preinstalled,
      }

    }
    /Ubuntu/: {
      include apt

      package { 'jdk':
        name => 'openjdk-8-jdk',
        ensure  => present,
        noop => $jdk_preinstalled,
      }
    }
    /(CentOS|Amazon|Fedora)/: {
      package { 'jdk':
        name => 'java-1.8.0-openjdk-devel',
        ensure => present,
        noop => $jdk_preinstalled,
      }
      if ($::operatingsystem == "Fedora") {
        file { '/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/security/cacerts':
          ensure => 'link',
          target => '/etc/pki/java/cacerts'
        }
      }
    }
    /OpenSuSE/: {
      package { 'jdk':
        name => 'java-1_8_0-openjdk-devel',
        ensure => present,
        noop => $jdk_preinstalled,
      }
    }
  }
}