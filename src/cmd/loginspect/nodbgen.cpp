#include "nodbgen.h"

#include "allocator.h"
#include "bf_tree.h"
#include "xct_logger.h"
#include "log_core.h"

void NoDBGen::setupOptions()
{
    boost::program_options::options_description opt("NoDBGen Options");
    opt.add_options()
        ("dbfile,d", po::value<string>(&dbfile)->required(),
            "Path to DB file")
        ("l,logdir", po::value<string>(&logdir)->required(),
            "Path to log directory")
    ;
    options.add(opt);
}

void NoDBGen::handlePage(fixable_page_h& p)
{
    sys_xct_section_t sx {false};
    Logger::log_p<page_img_format_log>(&p);
    sx.end_sys_xct(RCOK);
}

void NoDBGen::run()
{
    _options.set_string_option("sm_dbfile", dbfile);
    _options.set_string_option("sm_logdir", logdir);
    _options.set_bool_option("sm_vol_cluster_stores", true);

    smlevel_0::log = new log_core(_options);
    W_COERCE(smlevel_0::log->init());

    vol_t* vol = new vol_t(_options);
    smlevel_0::vol = vol;
    smlevel_0::bf = new bf_tree_m(_options);

    vol->build_caches(false);

    smlevel_0::bf->post_init();

    PageID pid = 0;
    PageID lastPID = vol->get_last_allocated_pid();
    PageID onePercent = lastPID / 100;

    generic_page page;
    fixable_page_h fixable;

    while (pid <= lastPID) {
        if (pid % onePercent == 0) {
            std::cout << "Pages processed: " << pid / onePercent << "%"  << std::endl;
        }

        if (vol->is_allocated_page(pid)) {
            W_COERCE(fixable.fix_direct(pid, LATCH_EX));
            handlePage(fixable);
            fixable.unfix(true /*evict*/);
        }
        pid++;
    }

    smlevel_0::bf->shutdown();
    delete smlevel_0::bf;

    vol->shutdown();
    delete vol;

    W_COERCE(smlevel_0::log->flush_all());
    (smlevel_0::log->shutdown());
    delete smlevel_0::log;
}

