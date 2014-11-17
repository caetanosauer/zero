= Building =

== Step 1: Required system libraries ==

If the optional Lintel features are disabled as shown above, only the following packages are needed (in Ubuntu 14.04):

`sudo apt-get install git cmake build-essential` for the basic building system

`sudo apt-get install liboost-dev libboost-thread-dev libboost-program-options-dev`

== Step 2: Download and build Lintel ==

Zero depends on the Lintel library, developed by HP Labs and distributed in the following URL:

`https://github.com/dataseries/Lintel`

Like Zero, Lintel uses cmake as base build system. One of the main features of cmake is that it generates build files and compiled binaries in a different directory than the source code. For example, you may clone the Lintel source and generate all build files in a `build` directory as follows:

```
git clone https://github.com/dataseries/Lintel.git

mkdir -p build/Lintel

cd build/Lintel

cmake \
    -D CMAKE_INSTALL_PREFIX=$PWD/.. \
    -D WITH_LIBXML2=OFF \
    -D WITH_PERL=OFF \
    -D WITH_LATEX=OFF \
    -D WITH_GNUPLOT=OFF \
    -D WITH_SGINFO=OFF \
    ../../Lintel
```

Since we don't need all features of Lintel, most of them can be disabled. This is done by setting the options `WITH_*=OFF` above. We also use the build directory as the install path, which means that all binaries and headers required for building Zero will be placed in `build` instead of a system-wide directory like `/usr`. This step is crucial in order for Zero to find all dependencies without manually editing its cmake files.

Finally, compile and install Lintel with:

```
make

make install
```

Optionally, you may delete the folder `build/Lintel` after invoking `make install`, since all required files are in the install path.

== Step 3: Download and build Zero ==

The procedure for building Zero is very similar to that of Lintel explained above. The following commands will download and build Zero. Make sure you first `cd` into your base development folder, i.e., where you cloned Lintel earlier.


```
git clone https://bitbucket.org/caetanosauer/Zero.git

mkdir -p build/Zero

cd build/Zero

cmake \
    -D CMAKE_INSTALL_PREFIX=$PWD/..
    ../../Zero

make

make install
```

== Notes for developers ==

Note that `make install` above is not required if you are just developing or testing Zero. Furthermore, cmake must only be invoked if you add new files or want to rebuild Zero or Lintel with different options. Otherwise, simply running `make` on the build folder is enough to recompile any files you changed.
