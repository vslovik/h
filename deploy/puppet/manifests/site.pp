require jdk
Class['jdk'] -> Service<||>

$provision_repo = hiera("matrixnorm::provision_repo", true)
if ($provision_repo) {
   require matrixnorm_repo
}

node default {
  $roles_enabled = hiera("matrixnorm::roles_enabled", false)

  if (!is_bool($roles_enabled)) {
    fail("matrixnorm::roles hiera conf is not of type boolean. It should be set to either true or false")
  }

  if ($roles_enabled) {
    include node_with_roles
  } else {
    include node_with_components
  }
}

if versioncmp($::puppetversion,'3.6.1') >= 0 {
  $allow_virtual_packages = hiera('matrixnorm::allow_virtual_packages',false)
  Package {
    allow_virtual => $allow_virtual_packages,
  }
}
