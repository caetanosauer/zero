#ifndef WEBSERVER_H
#define WEBSERVER_H

#include "command.h"
#include "kits_cmd.h"
#include "agglog.h"
#include <boost/asio.hpp>
#include <string>
#include <memory>
#include <thread>
#include <chrono>         // std::chrono::seconds

using namespace boost;
using namespace boost::system;
using namespace boost::asio;

class HandleKits
{
private:
    KitsCommand *kits;
    std::thread *t1;
    std::vector<std::string> countersJson;

public:
    HandleKits();
    int runKits(std::vector<std::string> options);
    void crash();
    void mediaFailure();
    void singlePageFailure();
    std::string getStats();
    std::string aggLog();
    std::string getCounters();
    std::string isRunning();
    std::string redoProgress();
    std::string logAnalysisProgress();
    std::string mediaRecoveryProgress();


    //void counters(std::vector<std::string> &countersJson);
};

class http_headers
{
private:
   std::string method;
   std::string url;
   std::string version;

   std::map<std::string, std::string> headers;
   std::map<std::string, std::string> options;
   std::vector<std::string> generate_kits_parameters();

public:
    http_headers();
    std::string get_response(HandleKits* kits);
    int content_length();
    void on_read_header(std::string line);
    void on_read_request_line(std::string line);
    void add_option(std::string, std::string);
};


class session {
   asio::streambuf buff;
   http_headers headers;

   static void read_body(std::shared_ptr<session> pThis, HandleKits* kits);

   static void read_next_line(std::shared_ptr<session> pThis, HandleKits* kits);

   static void read_first_line(std::shared_ptr<session> pThis, HandleKits* kits);
public:

   ip::tcp::socket socket;

   session(io_service& io_service)
      :socket(io_service)
   {
   }

   static void interact(std::shared_ptr<session> pThis, HandleKits* kits);

};

#endif