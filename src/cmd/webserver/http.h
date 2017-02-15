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
    bool kitsExecuted, kitsJustStarted;
    po::variables_map vm;
    std::vector<std::string> countersJson;

public:
    HandleKits();
    void runKits();
    void crash();
    void mediaFailure();
    void singlePageFailure();
    string getStats();
    string aggLog();
    string getCounters();
    string isRunning();

    //void counters(std::vector<std::string> &countersJson);
};

class http_headers
{
   std::string method;
   std::string url;
   std::string version;

   std::map<std::string, std::string> headers;
   std::map<std::string, std::string> options;

public:
    http_headers();
       std::string get_response(HandleKits &kits);

       int content_length();
       void on_read_header(std::string line);
       void on_read_request_line(std::string line);
       void add_option(std::string, std::string);

};


class session {
   asio::streambuf buff;
   http_headers headers;

   static void read_body(std::shared_ptr<session> pThis);

   static void read_next_line(std::shared_ptr<session> pThis, HandleKits &kits);

   static void read_first_line(std::shared_ptr<session> pThis, HandleKits &kits);
public:

   ip::tcp::socket socket;

   session(io_service& io_service)
      :socket(io_service)
   {
   }

   static void interact(std::shared_ptr<session> pThis, HandleKits &kits);

};

#endif
