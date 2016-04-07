#include "dbscan.h"

#include "allocator.h"

void DBScan::setupOptions()
{
    boost::program_options::options_description opt("DBScan Options");
    opt.add_options()
        ("dbfile,d", po::value<string>(&dbfile)->required(),
            "Path to DB file")
    ;
    options.add(opt);
}

void DBScan::handlePage(PageID pid, const generic_page& page, vol_t* vol)
{
    cout << "PID: " << pid;
    if (vol->is_allocated_page(pid)) {
        // CS TODO
        // w_assert0(pid == page.pid);
        if (pid == page.pid) {
            cout << " Store: " << page.store
                << " LSN: " << page.lsn;
        }
        else {
            cout << " MISMATCHED_PID_" << page.pid;
        }
    }
    else {
        cout << " unallocated";
    }
    cout << endl;
}

void DBScan::run()
{
    static constexpr size_t BUFSIZE = 1024;

    _options.set_string_option("sm_dbfile", dbfile);

    // CS TODO: manage SM sub-components with shared_ptr
    // auto vol = make_shared<vol_t>(_options);
    vol_t* vol = new vol_t(_options);
    smlevel_0::vol = vol;
    vol->build_caches(false);

    vector<generic_page, memalign_allocator<generic_page>> buffer(BUFSIZE);

    PageID pid = 0;
    PageID lastPID = vol->get_last_allocated_pid();

    while (pid <= lastPID) {
        size_t count = BUFSIZE;
        if (pid + count > lastPID) { count = lastPID - pid + 1; }
        W_COERCE(vol->read_many_pages(pid, &buffer[0], count));

        for (size_t i = 0; i < count; i++) {
            handlePage(pid++, buffer[i], vol);
        }
    }

    vol->shutdown(false);
    delete vol;
}
