class toolchain::scala {

  $scala_version = '2.11.8'
  $scala = "scala-${scala_version}"

  case $::operatingsystem {
    /Ubuntu|Debian/: {
      exec {'download-scala':
        command => "/usr/bin/wget http://www.scala-lang.org/files/archive/scala-${scala}.deb",
        cwd => '/usr/src',
        creates => "/usr/src/${scala}.deb"
      }
      package { 'scala':
        ensure => 'latest',
        source   => "/usr/src/${scala}.deb",
        provider => 'dpkg',
        require => Exec['download-scala']
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
