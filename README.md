# About

Zero is a transactional storage manager used mainly for prototyping in Database Systems research. It supports operations on a [Foster B-tree](http://dl.acm.org/citation.cfm?id=2338630) data structure with ACID semantics. Zero is designed for highly scalable, high-throughput OLTP. Being a research prototype, it does not provide certain usability features expected on a real production-level system.

# History

Zero is a fork of [Shore-MT](https://sites.google.com/site/shoremt/), which itself is derived from [Shore](http://research.cs.wisc.edu/shore/). The latter was developed in the early 90's by researchers in the University of Wisconsin-Madison, whereas the former was a continuation of the project at Carnegie Mellon University and, later on, EPFL. Shore-MT focuses on improving scalability in multi-core CPUs. Several published techniques in the database literature are based on Shore/Shore-MT, including recent and ongoing research.

Zero was developed at HP Labs. The initial milestone of the project was to implement the Foster B-tree data structure, thereby eliminating support for traditional ARIES-based B-trees and heap files. Zero also supports the Orthogonal Key-Value Locking Protocol and an improved Lock Manager design, as well as a novel swizzling technique that eliminates critical buffer manager overheads, deliviring performance comparable to in-memory systems despite being disk-based.

The latest developments in Zero are focused on Instant Recovery, a novel family of algorithms that extends the traditional ARIES design with on-demand recovery without blocking the system for new transactions, thereby significantly improving system availability. This [project](http://instantrecovery.github.io) is developed in a collaboration between HP Labs and the University of Kaiserslautern.

For a list of publications based on the Zero storage manager, see below.

# Project structure

The Zero source code directory is divided into three major parts:
- `src/common` Common utilities and infrastructure such as threads, latches, debug, etc.
- `src/sm` The Zero storage manager, providing an ACID-compliant key-value store based on Foster B-trees
- `src/cmd` Benchmarks (derived from [Shore-Kits](https://bitbucket.org/shoremt/shore-kits/src)) and utility programs packed in a tool called **zapps**

The storage manager consists of the following main components:
- The volume manager, which manages a file on which database pages are stored. Each page belongs to a *store* (i.e., a B-tree)
  - Main files: `vol.cpp`, `alloc_cache.cpp`, `stnode_cache.cpp`
- The buffer manager, which caches pages of a volume and keeps track of writes for transaction consistency
  - Main files: `bf_tree.cpp`, `bf_tree_cleaner.cpp`, `chkpt.cpp`
- The log manager, which manages a write-ahead log as well as its indexed log archive, used for some instant recovery techniques
  - Main files: `log_*.cpp`, `logarchiver.cpp`
- The Foster B-tree implementation
  - Main files: `btree.cpp`, `btree_*.cpp`, `btcursor.cpp`
- The transaction manager, which keeps track of active transactions and supports commit/abort functionality
  - Main files: `xct.cpp`
- The lock manager, which provides transaction isolation with orthogonal key-value locking
  - Main files: `lock.cpp`, `lock_*.cpp`
- The recovery manager, which provides ACID-compliant recovery from system and media failures with instant recovery
  - Main files: `restart.cpp`, `restore.cpp`, `chkpt.cpp`, page-based REDO logic in `bf_tree.cpp`
- The top-level SM interface: `sm.cpp`

# Building

## Dependencies

Zero is developed in C++ and uses the CMake building system. Its only dependency is on a few Boost libraries, widely available in the major Linux distributions. Currently, is it supported only on Linux.

On an Ubuntu system, the dependencies can usually be installed with the following commands:

```
sudo apt-get install git cmake build-essential
sudo apt-get install liboost-dev libboost-thread-dev libboost-program-options-dev libboost-random-dev
```

Zero requires libboost version 1.48. Please make sure that this version or a higher one is installed.

## Compilation

CMake supports out-of-source builds, which means that binaries are generated in a different directory than the source files. This not only maintains a clean source directory, but also allows multiple coexisting builds with different configurations.

The typical approach is to create a `build` folder inside the project root after cloning it with git:

```
git clone https://github.com/caetanosauer/zero
cd zero
mkdir build
```

To generate the build files, type:

```
cd build
cmake ..
```

Alternatively, a debug version without optimizations is also supported:

```
cmake -DCMAKE_BUILD_TYPE=Debug ..
```

Finally, to compile:

```
make -j <number_of_cores> sm
```

The `-j` flag enables compilation in parallel on multi-core CPUs. It is a standard feature of the Make building system. The `sm` target builds the static storage manager library, `libsm`.

To use the benchmarks and utilities, compile the *zapps* binary with:

```
make -j <number_of_cores> zapps
```

The binary is stored in the `src/cmd` subfolder. As an example, a TPC-C benchmark can be loaded and executed for 60 seconds with:

```
src/cmd/zapps kits -b tpcc --load --duration 60
```

For reference on the commands supported and their arguments, see the source files `src/cmd/base/command.cpp` and `src/cmd/kits/kits_cmd.cpp`

## Testing

## Current status
> Since February 2016, the test cases are not being maintained.

Zero is designed to be used as a library for transactional applications. As such, there is no program to be executed after compilation. The generated storage manager library is `libsm.a`. However, the test suite can be ran with:

```
make test
```

It uses the Google Test libraries, which are embedded in the source code.

# Publications

The following publications describe novel techniques which were implemented and evaluated on Zero:

* Caetano Sauer: [Modern techniques for transaction-oriented database recovery](http://wwwlgis.informatik.uni-kl.de/cms/fileadmin/publications/2017/PhD_Thesis_Caetano_Sauer.pdf) -- My PhD thesis
* Caetano Sauer, Goetz Graefe, Theo Härder [Instant restore after a media failure](https://arxiv.org/pdf/1702.08042.pdf). Link to an extended preprint version. Published on ADBIS 2017.
* Caetano Sauer, Lucas Lersch, Theo Härder, Goetz Graefe [Update propagation strategies for high-performance OLTP](http://wwwlgis.informatik.uni-kl.de/cms/typo3/sysext/cms/tslib/media/fileicons/pdf.gif). ADBIS 2016 (best-paper award), LNCS 9809, pp. 152-165
Springer-Verlag, August 2016 
* Caetano Sauer, Goetz Graefe, Theo Härder: [Single-pass restore after a media failure](http://btw-2015.de/res/proceedings/Hauptband/Wiss/Sauer-Single-pass_restore_after_a.pdf). BTW 2015:217-236 [[Slides](http://btw-2015.de/res/slides/Sauer-Single-pass_restore_after_a_m_slides.pdf)]
* Goetz Graefe, Hideaki Kimura: [Orthogonal key-value locking](http://btw-2015.de/res/proceedings/Hauptband/Wiss/Graefe-Orthogonal_key-value_locking.pdf). BTW 2015:237-256 [[Slides](http://btw-2015.de/res/slides/Graefe-Orthogonal_key-value_locking_slides.pdf)]
* Goetz Graefe, Haris Volos, Hideaki Kimura, Harumi A. Kuno, Joseph Tucek, Mark Lillibridge, Alistair C. Veitch: [In-Memory Performance for Big Data](http://www.vldb.org/pvldb/vol8/p37-graefe.pdf). PVLDB 8(1):37-48 (2014)
* Goetz Graefe, Mark Lillibridge, Harumi A. Kuno, Joseph Tucek, Alistair C. Veitch: [Controlled lock violation](http://doi.acm.org/10.1145/2463676.2465325). SIGMOD 2013:85-96
* Goetz Graefe, Hideaki Kimura, Harumi A. Kuno: [Foster b-trees](http://doi.acm.org/10.1145/2338626.2338630). ACM Trans. Database Syst. (TODS) 37(3):17 (2012)
* Hideaki Kimura, Goetz Graefe, Harumi A. Kuno: [Efficient Locking Techniques for Databases on Modern Hardware](http://www.adms-conf.org/kimura_adms12.pdf). ADMS@VLDB 2012:1-12
* Goetz Graefe, Harumi A. Kuno: [Definition, Detection, and Recovery of Single-Page Failures, a Fourth Class of Database Failures](http://vldb.org/pvldb/vol5/p646_goetzgraefe_vldb2012.pdf). PVLDB 5(7):646-655 (2012)

# Contact

This repository is maintained by Caetano Sauer at the University of Kaiserslautern. If you wish to experiment with Zero, feel free to contact me at `csauer@cs.uni-kl.de`. Being a research prototype used by many developers over the course of two decades, getting started in the code may be a daunting task, due to the lack of thorough documentation and the high heterogeneity of the code.
