### Aside: Why aren't we using filebench as a submodule?

We meant to use git submobule. However, we encountered some problems when trying this.
The code downloaded from git repo cannot be built directly. It requires runing
automake, autoconf, libtoolize, aclocal and etc to generate the configure file.
The testing VMs used by Jenkins are not able to install the required version of
these utilities. We may need to roll forward with Ubuntu version or and reconsider
git submodule in the future. Therefore, we just download the release version provided
on filebench github website. It includes a configure file already. 


## Retrieve and build filebench

```shell
./install.sh
```


## Run filebench

```shell
sudo ../../setup-ftfs.sh
sudo ./run-test.sh ${workload}
```

`workload` should be one of,
- fileserver
- oltp
- webproxy
- webserver

without the .f extension, since run-test.sh adds the extension.

**IMPORTANT**: filebench requires address space layout randomization (ASLR) to
be disabled, so use `run-test.sh` instead of invoking filebench directly.
`run-test.sh` disables ASLR before running filebench, and then restores the
system state after filebench is done.


## Cleanup

```shell
sudo ../../cleanup-fs.sh
```
