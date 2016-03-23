#include "logstats.h"

class LogStatsHandler : public Handler
{
private:
    bool isArchive;

public:

    LogStatsHandler(bool isArchive) : isArchive(isArchive)
    {}

    virtual void invoke(logrec_t& /*r*/)
    {
        // TODO implement interesting stats such as
        // avg logrec size
        // number of logrecs
        // histogram (of count and size) of logrec types
    }

    virtual void finalize() {};
};

void LogStats::setupOptions()
{
    LogScannerCommand::setupOptions();
    options.add_options()
        ("index,i", po::value<bool>(&indexOnly)->default_value(false)
         ->implicit_value(true),
         "Show only information about log archive index (works with -a only)")
        ;
}

void LogStats::run()
{
    LogStatsHandler* h = new LogStatsHandler(isArchive);
    BaseScanner* s = getScanner();

    s->add_handler(h);
    s->fork();
    s->join();

    delete s;
    delete h;

    if (isArchive) {
        // Gather physical stats of each run file
        vector<string> files;

        // TODO assuming default block size
        LogArchiver::ArchiveDirectory dir(logdir,
                LogArchiver::DFT_BLOCK_SIZE);

        if (filename.empty()) {
            dir.listFiles(files);
        }
        else {
            files.push_back(filename);
        }

        // buffer for log archive blocks
        char buffer[LogArchiver::DFT_BLOCK_SIZE];
        int fd = -1;
        size_t fpos = 0;
        size_t currRun = 0;
        size_t blockCount = 0;
        size_t indexBlockCount = 0;
        size_t bpos = 0;
        size_t blockEnd = 0;
        logrec_t* lr = NULL;
        PageID lastPID = 0;
        size_t currBlock = 0;
        size_t pidCount = 0;

        int flags = smthread_t::OPEN_RDONLY;

#if 0 // CS TODO: does not work with new archive contiguous format
        if (!indexOnly) {
            for (size_t i = 0; i < files.size(); i++) {
                W_COERCE(me()->open((logdir + "/" + files[i]).c_str(),
                            flags, 0744, fd));
                fpos = 0;
                currBlock = 0;

                W_COERCE(dir.getIndex()->getBlockCounts(fd, &indexBlockCount,
                            &blockCount));

                while(currBlock < blockCount) {
                    dir.readBlock(fd, buffer, fpos);
                    if (fpos == 0) { break; }

                    bpos = sizeof(LogArchiver::BlockAssembly::BlockHeader);
                    blockEnd = LogArchiver::BlockAssembly::getEndOfBlock(buffer);
                    lastPID = lpid_t::null;
                    pidCount = 0;

                    while (bpos < blockEnd) {
                        lr = (logrec_t*) (buffer + bpos);
                        w_assert1(lr->valid_header(lr->lsn_ck()));

                        if (lr->pid() != lastPID) {
                            pidCount++;
                            lastPID = lr->pid();
                        }

                        bpos += lr->length();
                    }

                    cout << "run=" << currRun << " block=" << currBlock
                        << " pids=" << pidCount << endl;

                    currBlock++;
                }

                W_COERCE(me()->close(fd));
                currRun++;
            }
        }
#endif

        cout << "INDEX INFO" << endl;

        LogArchiver::ArchiveIndex * archIndex = dir.getIndex();
        archIndex->dumpIndex(cout);
    }
}

