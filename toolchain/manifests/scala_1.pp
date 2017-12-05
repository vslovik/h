# https://github.com/gini/puppet-scala/blob/master/manifests/params.pp
# https://stackoverflow.com/questions/33953042/cant-provision-scala-sbt-from-same-file-as-jenkins-service
class toolchain::scala(
  $version        = '2.11.8',
  $url            = 'org.scala-lang',
  $package_format = 'scala-library',
) {

  archive::download { "scala-${version}.${package_format}":
    url        => "http://www.scala-lang.org/files/archive/scala-${version}.${package_format}",
    checksum   => false,
    src_target => '/var/tmp',
  }

  $package_provider = $::osfamily ? {
    'RedHat' => 'rpm',
    'Debian' => 'dpkg',
    default  => fail('Unsupported OS family'),
  }

  package { "scala-${version}":
    ensure   => installed,
    provider => $package_provider,
    source   => "/var/tmp/scala-${version}.${package_format}",
    require  => Archive::Download["scala-${version}.${package_format}"],
  }

  ####
  wget::fetch { "download scala":
    source => "http://www.scala-lang.org/files/archive/scala-2.10.3.deb",
    destination => "/tmp/scala-2.10.3.deb",
    cache_dir => "/vagrant/cache/"
  }

  package { "libjansi-java": }

  package { "scala":
    provider => "dpkg",
    source => "/tmp/scala-2.10.3.deb",
    require => [ File["/tmp/scala-2.10.3.deb"], Package["openjdk-8-jdk"], Package["libjansi-java"] ]
  }
}
