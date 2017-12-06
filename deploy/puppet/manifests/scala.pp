class toolchain::scala {

  $scala_version = '2.12.4'
  $scala = "scala-${scala_version}"

  case $::operatingsystem {
    /Ubuntu|Debian/: {
      exec {'download-scala':
        command => "/usr/bin/wget http://www.scala-lang.org/files/archive/${scala}.deb",
        cwd => '/usr/src',
        creates => "/usr/src/${scala}.deb"
      }
      package { 'scala':
        ensure => 'latest',
        source   => "/usr/src/${scala}.deb",
        provider => 'dpkg',
        require => [ Exec['download-scala'], Package["openjdk-8-jdk"] ]
      }
    }
    default: {
      package { 'scala':
        ensure => 'latest',
        source => "http://downloads.lightbend.com/scala/${scala_version}/${scala}.rpm",
        provider => 'rpm'
      }
    }
  }
}
