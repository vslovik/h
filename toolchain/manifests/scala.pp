# https://github.com/gini/puppet-scala/blob/master/manifests/params.pp
class toolchain::scala(
  $version        = 'UNSET',
  $url            = 'UNSET',
  $package_format = 'UNSET',
) {

  include scala::params

  $version_real = $version ? {
    'UNSET' => $::scala::params::version,
    default => $version,
  }

  $package_format_real = $package_format ? {
    'UNSET' => $::scala::params::package_format,
    default => $package_format,
  }

  $url_real = $url ? {
    'UNSET' => "http://www.scala-lang.org/files/archive/scala-${version_real}.${package_format_real}",
    default => $url,
  }

  archive::download { "scala-${version_real}.${package_format_real}":
    url        => $url_real,
    checksum   => false,
    src_target => '/var/tmp',
  }

  $package_provider = $::osfamily ? {
    'RedHat' => 'rpm',
    'Debian' => 'dpkg',
    default  => fail('Unsupported OS family'),
  }

  package { "scala-${version_real}":
    ensure   => installed,
    provider => $package_provider,
    source   => "/var/tmp/scala-${version_real}.${package_format_real}",
    require  => Archive::Download["scala-${version_real}.${package_format_real}"],
  }
}
