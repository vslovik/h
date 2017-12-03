# Puppet classes for deploying Hadoop

## Intro

BigTop is now using Puppet 3.x (BIGTOP-1047)!

The puppet classes for deployment setup and deploy hadoop services.
This includes tasks such as:

- service installation
- pointing slaves to masters (i.e. regionservers, nodemanagers to their respective master)
- starting the services

The mode of puppet is *masterless*: there is no fancy coordination happening behind the scenes.

Puppet has a notion of a configuration directory, called config.  
When running puppet apply, note that puppet's confdir is *underneath* the `--confdir` value.
For example: If you have `site.csv` in `/etc/puppet/config`, 
Then you should use `--confdir=/etc/puppet` , and puppet finds the config dir underneath.

## Debugging

If in any case, you need to debug these recipes, you can add `notify("...")` statements into 
the puppet scripts.  

In time, we will add more logging and debugging to these recipes.  Feel free to submit 
a patch for this!

## Configuration

As above, we defined a confdir (i.e. `/etc/puppet/`) which has a `config/` directory in it.

The heart of puppet is the manifests file.  This file (`manifests/init.pp`) 
expects configuration to live in hiera as specified by `$confdir/hiera.yaml`. An example
`hiera.yaml` as well as hiera configuration yaml files are provided with classes. They
basically take the form:

```
key: value
```

with syntactic variations for hashes and arrays. Please consult the excellent puppet and hiera
documentation for details.

All configuration is done via such key value assignments in `hierdata/site.yaml`. Any options
not defined there will revert to a default value defined in `hieradata/cluster.yaml`, with the
following exceptions (which are required):

* `matrixnorm::hadoop_head_node`: must be set to the FQDN of the name node of your
   cluster (which will also become its job tracker and gateway)

* `matrixnorm::matrixnorm_repo_uri`: uri of a repository containing packages for
   hadoop as built by Bigtop.

`$confdir` is the directory that puppet will look into for its configuration.  On most systems, 
this will be either `/etc/puppet/` or `/etc/puppetlabs/puppet/`.  You may override this value by 
specifying `--confdir=path/to/config/dir` on the puppet command line.

`cluster.yaml` also serves as an example what parameters can be set and how they usually interact
between modules.

You can instruct the recipes to install ssh-keys for user hdfs to enable passwordless login
across the cluster. This is for test purposes only, so by default the option is turned off.

Files such as ssh-keys are imported from the master using the `puppet:///` URL scheme. For this
to work, fileserver has to be enabled on the puppet master, the files module enabled and access
allowed in auth.conf. fileserver.conf should therefore contain e.g.:

```
[files]
  path /etc/puppet/files
  allow *
```

No changes are required to the default puppet 3 auth.conf.

For other options that may be set here, look for class parameters in the modules'
manifests/init.pp files. Any class parameter can be used as a hiera key if prefixed with the
module and class namespace. Module hue's server class will look for its parameter rm_host as
`hue::server::rm_host` in hiera.
Note that if `hadoop::hadoop_storage_dirs` is left unset, puppet will attempt to guess which
directories to use.

## Usage

- Make sure that the deploy directory is available on every node of your cluster
- Make sure puppet is installed
- Make sure all the required puppet modules are installed:

```
gradle toolchain-puppetmodules # if you already have JAVA installed
```

or

```
puppet apply --modulepath=<path_to_matrixnorm> -e "include matrixnorm_toolchain::puppet-modules"
```

This will install the following module(s) for you:

  * [puppet stdlib module](https://forge.puppetlabs.com/puppetlabs/stdlib)
  * [puppet apt module](https://forge.puppetlabs.com/puppetlabs/apt) on Ubuntu, Debian only

Note that, the puppet apt module version must be equal to or higher than 2.0.1 after BIGTOP-1870.
Bigtop toolchan can take care of that for you, so just be aware of it.

And run the following on those nodes:

```
cp deploy/puppet/hiera.yaml /etc/puppet
mkdir -p /etc/puppet/hieradata
rsync -a --delete deploy/puppet/hieradata/site.yaml deploy/puppet/hieradata/matrixnorm /etc/puppet/hieradata/
```
Edit /etc/puppet/hieradata/site.yaml to your liking, setting up the hostname for
hadoop head node, path to storage directories and their number, list of the components
you wish to install, and repo URL. At the end, the file will look something like this

```
matrixnorm::hadoop_head_node: "hadoopmaster.example.com"
hadoop::hadoop_storage_dirs:
  - "/data/1"
  - "/data/2"
hadoop_cluster_node::cluster_components:
  - ignite_hadoop
  - hive
  - spark
  - yarn
  - zookeeper
matrixnorm::matrixnorm_repo_uri: "http://bigtop-repos.s3.amazonaws.com/releases/1.2.0/ubuntu/16.04/x86_64"
```

And finally execute
```
puppet apply -d --parser future --modulepath="matrixnorm-deploy/puppet/modules:/etc/puppet/modules" matrixnorm-deploy/puppet/manifests
```
When ignite-hadoop accelerator is deployed the client configs are placed under
`/etc/hadoop/ignite.client.conf`. All one needs to do to run Mapreduce jobs on ignite-hadoop grid
is to set `HADOOP_CONF_DIR=/etc/hadoop/ignite.client.conf` in the client session.

## Passwords

These classes are mostly used for regression testing. For ease of use they
contain insecure default passwords in a number of places. If you intend to use
them in production environments, make sure to track down all those places and
set proper passwords. This can be done using the corresponding hiera settings.
Some of these (but almost certainly not all!) are:

```
hadoop::common_hdfs::hadoop_http_authentication_signature_secret
hadoop::httpfs::secret
```