#include "dbscan.h"

#include "allocator.h"
#include "bf_tree.h"

void DBScan::setupOptions()
{
    boost::program_options::options_description opt("DBScan Options");
    opt.add_options()
        ("dbfile,d", po::value<string>(&dbfile)->required(),
            "Path to DB file")
        ("pid", po::value<PageID>(&req_pid)->default_value(0),
            "Read only this PID")
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
    _options.set_bool_option("sm_vol_cluster_stores", true);

    vol_t* vol = new vol_t(_options);
    smlevel_0::vol = vol;
    smlevel_0::bf = new bf_tree_m(_options);

    vol->build_caches(false);

    smlevel_0::bf->post_init();

    vector<generic_page, memalign_allocator<generic_page>> buffer(BUFSIZE);

    if (req_pid == 0) {
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
    }
    else {
        W_COERCE(vol->read_many_pages(req_pid, &buffer[0], 1));
        handlePage(req_pid, buffer[0], vol);
    }

    vol->shutdown();
    delete vol;
}
