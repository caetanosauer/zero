#include "dbinspect.h"

#include "alloc_cache.h"

void DBInspect::setupOptions()
{
    po::options_description opt("DBInspect Options");
    opt.add_options()
        ("file,f", po::value<string>(&file)->required(),
         "DB file to be inspected")
        ;
    options.add(opt);
}

void DBInspect::run()
{
    // Build alloc_cache to get allocation status of pages
    int fd;
    int flags = smthread_t::OPEN_RDONLY;
    W_COERCE(me()->open(file.c_str(), flags, 0744, fd));

    filestat_t fs;
    W_COERCE(me()->fstat(fd, fs));
    uint64_t fsize = fs.st_size;
    PageID max_pid = fsize / sizeof(generic_page);

    // CS TODO: update for new alloc cache
    // bf_fixed_m bf_fixed(NULL, fd, max_pid);
    // cout << "Max pid = " << max_pid << endl;
    // bf_fixed.init();
    // alloc_cache_t alloc(&bf_fixed);
    // alloc.load_by_scan(max_pid);

    // Iterate and print info about pages
    ifstream in(file, std::ifstream::binary);
    generic_page page;

    // Volume header page can be just printed out
    in.seekg(0);
    in.read((char*) &page, sizeof(generic_page));
    cout << (char*) &page << endl;

    PageID p = 1;
    while (in) {
        in.seekg(p * sizeof(generic_page));
        in.read((char*) &page, sizeof(generic_page));
        if (!in) { break; }

        cout << "Page=" << p
            << " PID=" << page.pid
            << " LSN=" << page.lsn
            << " Checksum="
            << (page.checksum == page.calculate_checksum() ? "OK" : "WRONG")
            // << " Alloc=" << (alloc.is_allocated_page(p) ? "YES" : "NO")
            << endl;
        p++;
    }
}

