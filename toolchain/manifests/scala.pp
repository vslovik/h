# https://github.com/gini/puppet-scala/blob/master/manifests/params.pp
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
}
