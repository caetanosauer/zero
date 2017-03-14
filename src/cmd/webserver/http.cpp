#include "http.h"
#include <time.h>
#include <string.h>


http_headers::http_headers ()
{
}
std::string http_headers::get_response(HandleKits* kits)
{
  std::stringstream ssOut;
  if(url == "/favicon.ico")
  {
     int nSize = 0;

     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "content-type: image/vnd.microsoft.icon" << std::endl;
     ssOut << "content-length: " << nSize << std::endl;
     ssOut << std::endl;
  }
  else if(url == "/")
  {
     std::string sHTML = "<html><body><h1>Hello World</h1><p>This is a test web server in c++</p></body></html>";
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "content-type: text/html" << std::endl;
     ssOut << "content-length: " << sHTML.length() << std::endl;
     ssOut << std::endl;
     ssOut << sHTML;
  }
  else if(url == "/startkits")
  {
     std::string sHTML = "{\"kitsStart\":true}";
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "Access-Control-Allow-Origin: *" << std::endl;
     ssOut << "content-type: application/json" << std::endl;
     ssOut << "content-length: " << sHTML.length() << std::endl;
     ssOut << std::endl;
     ssOut << sHTML;
     std::vector<std::string> kitsParameters = generate_kits_parameters();
     int result = kits->runKits(kitsParameters);
  }
  else if(url == "/counters")
  {
     string json = kits->getCounters();

     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "Access-Control-Allow-Origin: *" << std::endl;
     ssOut << "content-type: application/json" << std::endl;
     ssOut << "content-length: " << json.length() << std::endl;
     ssOut << std::endl;
     ssOut <<   json;
  }
  else if(url == "/getstats")
  {
     string json = kits->getStats();
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "Access-Control-Allow-Origin: *" << std::endl;
     ssOut << "content-type: application/json" << std::endl;
     ssOut << "content-length: " << json.length() << std::endl;
     ssOut << std::endl;
     ssOut <<   json;
  }
  else if(url == "/agglog")
  {
     string json = kits->aggLog();
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "Access-Control-Allow-Origin: *" << std::endl;
     ssOut << "content-type: application/json" << std::endl;
     ssOut << "content-length: " << json.length() << std::endl;
     ssOut << std::endl;
     ssOut <<   json;
  }
  else if(url == "/iskitsrunning")
  {
     string json = kits->isRunning();
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "Access-Control-Allow-Origin: *" << std::endl;
     ssOut << "content-type: application/json" << std::endl;
     ssOut << "content-length: " << json.length() << std::endl;
     ssOut << std::endl;
     ssOut <<   json;
  }
  else if(url == "/crash")
  {
      kits->crash();
      std::string sHTML = "{\"hasCrashed\":true}";
      ssOut << "HTTP/1.1 200 OK" << std::endl;
      ssOut << "Access-Control-Allow-Origin: *" << std::endl;
      ssOut << "content-type: application/json" << std::endl;
      ssOut << "content-length: " << sHTML.length() << std::endl;
      ssOut << std::endl;
      ssOut << sHTML;
  }
  else if(url == "/mediafailure")
  {
      kits->mediaFailure();
      std::string sHTML = "{\"hasMediaFailured\":true}";
      ssOut << "HTTP/1.1 200 OK" << std::endl;
      ssOut << "Access-Control-Allow-Origin: *" << std::endl;
      ssOut << "content-type: application/json" << std::endl;
      ssOut << "content-length: " << sHTML.length() << std::endl;
      ssOut << std::endl;
      ssOut << sHTML;
  }
  else if(url == "/singlepagefailure")
  {
      kits->singlePageFailure();
      std::string sHTML = "{\"hasPageFailed\":true}";
      ssOut << "HTTP/1.1 200 OK" << std::endl;
      ssOut << "Access-Control-Allow-Origin: *" << std::endl;
      ssOut << "content-type: application/json" << std::endl;
      ssOut << "content-length: " << sHTML.length() << std::endl;
      ssOut << std::endl;
      ssOut << sHTML;
  }
  else if(url == "/recoveryprogress")
  {
      std::string sHTML = "{\"redoProgress\":" + kits->redoProgress();
      sHTML +=", \"logAnalysisProgress\":" + kits->logAnalysisProgress() +"}";
      ssOut << "HTTP/1.1 200 OK" << std::endl;
      ssOut << "Access-Control-Allow-Origin: *" << std::endl;
      ssOut << "content-type: application/json" << std::endl;
      ssOut << "content-length: " << sHTML.length() << std::endl;
      ssOut << std::endl;
      ssOut << sHTML;
  }
  else if(url == "/mediarecoveryprogress")
  {
      std::string sHTML = "{\"mediaRecoveryProgress\":" + kits->mediaRecoveryProgress() +"}";
      ssOut << "HTTP/1.1 200 OK" << std::endl;
      ssOut << "Access-Control-Allow-Origin: *" << std::endl;
      ssOut << "content-type: application/json" << std::endl;
      ssOut << "content-length: " << sHTML.length() << std::endl;
      ssOut << std::endl;
      ssOut << sHTML;
  }
  else
  {
     std::string sHTML = "<html><body><h1>404 Not Found</h1><p>There's nothing here.</p></body></html>";
     ssOut << "HTTP/1.1 404 Not Found" << std::endl;
     ssOut << "content-type: text/html" << std::endl;
     ssOut << "content-length: " << sHTML.length() << std::endl;
     ssOut << std::endl;
     ssOut << sHTML;
  }
  return ssOut.str();
};

int http_headers::content_length()
{
  if(headers.find("Content-Length") != headers.end())
  {
     std::stringstream ssLength(headers.find("Content-Length")->second);
     int content_length;
     ssLength >> content_length;
     return content_length;
  }
  return 0;
};

void http_headers::on_read_header(std::string line)
{

  std::stringstream ssHeader(line);
  std::string headerName;
  std::getline(ssHeader, headerName, ':');

  std::string value;
  std::getline(ssHeader, value);
  headers[headerName] = value;
};

void http_headers::on_read_request_line(std::string line)
{
  std::stringstream ssRequestLine(line);
  ssRequestLine >> method;
  ssRequestLine >> url;
  ssRequestLine >> version;
   std::cout << "request for resource: " << url << std::endl;
};

void http_headers::add_option(std::string key, std::string value)
{
    std::cout << "key: " << key << std::endl;
    std::cout << "value: " << value << std::endl;
    options[key] = value;
};

std::vector<std::string> http_headers::generate_kits_parameters()
{
    std::vector<std::string> parameters;
    if ((options.find("benchmark") != options.end()) && options.find("threads") != options.end()) {
        parameters.push_back("-b");
        parameters.push_back(options["benchmark"]);
        parameters.push_back("-t");
        parameters.push_back(options["threads"]);
    }
    else
    {
        std::cout << "ERROR: Missing parameters 'thread' and 'benchmark'" << std::endl;
        return parameters;
    }

    if (options.find("no_stop") != options.end()) {
        parameters.push_back("--no_stop");
    }
    else if (options.find("duration") != options.end()) {
        parameters.push_back("--duration");
        parameters.push_back(options["duration"]);
    }
    else if (options.find("transactions") != options.end()) {
        parameters.push_back("--trxs");
        parameters.push_back(options["transactions"]);
    }

    if (options.find("load") != options.end()) {
        parameters.push_back("--load");

    }

    for (int i = 0; i < parameters.size(); i++)
        std::cout << parameters.at(i) << std::endl;
    return parameters;

}

void session::read_body(std::shared_ptr<session> pThis, HandleKits* kits) {

    std::cout << "read_body" << std::endl;

    size_t nbuffer = pThis->headers.content_length();
    std::cout << __FILE__ << ":" << __LINE__ << " nbuffer: " << nbuffer << "\n";

    std::shared_ptr<std::vector<char> > bufptr = std::make_shared<std::vector<char> >(nbuffer);

    auto partial = std::copy(
            std::istreambuf_iterator<char>(&pThis->buff), {},
            bufptr->begin());

    std::size_t already_received = std::distance(bufptr->begin(), partial);

    assert(nbuffer >= already_received);
    nbuffer -= already_received;

    asio::async_read(pThis->socket, boost::asio::buffer(&*bufptr->begin() + already_received, nbuffer),
        [=](const boost::system::error_code& e, std::size_t s) {
            // EOF is to be expected on client disconnect
            if (e && e != boost::asio::error::eof) {
                std::cerr << "Error:" << __LINE__ << " " << e.message() << "\n"; return;
            }

            std::string body(&*bufptr->begin(), already_received + s);

            std::string::size_type p = 0;
            for (int i = 0; i<2; ++i)
                p = body.find_last_of("\r\n", p-1);

            std::cout << "Tail: '" << body.substr(p+1) << "'\n";

            std:stringstream bodyStream;
            bodyStream << body;
            std::string key, value;
            while(std::getline(bodyStream, key, ':')) {
                key.erase(std::remove(key.begin(), key.end(), ' '),key.end());
                std::getline(bodyStream, value, ',');
                value.erase(std::remove(value.begin(), value.end(), ' '),value.end());
                pThis->headers.add_option(key,value);
            }
            std::shared_ptr<std::string> str = std::make_shared<std::string>(pThis->headers.get_response(kits));
            asio::async_write(pThis->socket, boost::asio::buffer(str->c_str(), str->length()), [pThis, str](const boost::system::error_code& e, std::size_t s)
            {
               //std::cout << "done" << std::endl;
            });
        });

}

void session::read_next_line(std::shared_ptr<session> pThis, HandleKits* kits)
{
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis, kits](const boost::system::error_code& e, std::size_t s)
  {
     std::string line, ignore;
     std::istream stream {&pThis->buff};
     std::getline(stream, line, '\r');
     std::getline(stream, ignore, '\n');
     pThis->headers.on_read_header(line);

     if(line.length() == 0)
     {
        if(pThis->headers.content_length() == 0)
        {
           std::shared_ptr<std::string> str = std::make_shared<std::string>(pThis->headers.get_response(kits));
           asio::async_write(pThis->socket, boost::asio::buffer(str->c_str(), str->length()), [pThis, str](const boost::system::error_code& e, std::size_t s)
           {
              //std::cout << "done" << std::endl;
           });
        }
        else
        {
           pThis->read_body(pThis, kits);
        }
     }
     else
     {
        pThis->read_next_line(pThis, kits);
     }
  });
};

void session::read_first_line(std::shared_ptr<session> pThis, HandleKits* kits)
{
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis, kits](const boost::system::error_code& e, std::size_t s)
  {
     std::string line, ignore;
     std::istream stream {&pThis->buff};
     std::getline(stream, line, '\r');
     std::getline(stream, ignore, '\n');
     pThis->headers.on_read_request_line(line);
     pThis->read_next_line(pThis, kits);
  });
};

void session::interact(std::shared_ptr<session> pThis, HandleKits* kits)
{
  read_first_line(pThis, kits);
};


HandleKits::HandleKits():
kitsExecuted(false),
kitsJustStarted(false)
{
    kits = new KitsCommand();
    kits->setupOptions();
}


void counters(std::vector<std::string> &counters, KitsCommand *kits)
{
    //wait for kits runs
    while(!kits->running());
    //kits started!
    std::stringstream ssOut;
    kits->getShoreEnv()->gatherstats_sm(ssOut);
    string varJson, parJson;
    while (ssOut >> varJson) {
        ssOut >> parJson;
        counters.push_back("\"" + varJson + "\" :  [0, ");
    }

    while(kits->running()) {
        //wait 1 second
        std::this_thread::sleep_for (std::chrono::seconds(1));
        std::stringstream ss;
        kits->getShoreEnv()->gatherstats_sm(ss);
        for(int i = 0; i < counters.size(); i++) {
            ss >> varJson;
            ss >> parJson;
            counters[i]+=(parJson + ", ");
        }

        //string strReturn = ssReturn.str();
        //strReturn[strReturn.length()-1] = '\0';
        //strReturn[strReturn.length()-2] = '}';
    }
};


int HandleKits::runKits(std::vector<std::string> options)
{
    if (kits->running()) {
        std::cout << "kits running!!!!" << std::endl;
        kits->join();
    }

    if (kitsExecuted) {
        delete kits;
        kits = new KitsCommand();
        kits->setupOptions();
    }

    // the options should include at least the benchmark and the number of threads e.g.: -b tpcc -t 2
    if (options.size() < 4 || kits->running())
        return -1;

    int predefinedArgs = 4;
    int argc=options.size() + predefinedArgs;
    char* argv[argc];//={"zapps", "kits", "-b", "tpcc", "--no_stop", "-t", "1", "--sm_archiving", "true"};
    argv[0] = (char*)malloc(strlen("zapps") + 1);
    strcpy(argv[0],"zapps");
    argv[1] = (char*)malloc(strlen("kits") + 1);
    strcpy(argv[1],"kits");
    argv[2] = (char*)malloc(strlen("--sm_archiving") + 1);
    strcpy(argv[2],"--sm_archiving");
    argv[3] = (char*)malloc(strlen("true") + 1);
    strcpy(argv[3],"true");

    for (int i = predefinedArgs; i < argc; i++){
        argv[i] = (char*) malloc(strlen(options.at(i-predefinedArgs).c_str()) + 1);
        strcpy(argv[i], options.at(i-predefinedArgs).c_str());
    }
    std::cout <<"size: " << argc << std::endl;
    for (int i = 0; i < argc ; i++)
        std::cout << "aaaaa" << argv[i] << std::endl;
    po::store(po::parse_command_line(argc,argv,kits->getOptions()), vm);
    po::notify(vm);
    kits->setOptionValues(vm);

    kits->fork();

    kitsExecuted = true;
    kitsJustStarted = true;

    t1 = new std::thread (counters, std::ref(countersJson), kits);
};

void HandleKits::crash()
{
    if (kits->running()) {
        kits->crashFilthy();
    }
}

void HandleKits::mediaFailure()
{
    if (kits->running()) {
        kits->mediaFailure(0);
    }
}

void HandleKits::singlePageFailure()
{
    if (kits->running()) {
        kits->randomRootPageFailure();
    }
}

std::string HandleKits::redoProgress()
{
    std::string progress = "0";
    if (kits->getShoreEnv()->has_log_analysis_finished()) {
        size_t pToRecover = kits->getShoreEnv()->get_total_pages_to_recover();
        size_t pRecovered = kits->getShoreEnv()->get_total_pages_redone();

        if (pRecovered < pToRecover)
            progress = std::to_string((static_cast<double>(pRecovered)/static_cast<double>(pToRecover))*100);
        else if (pRecovered > pToRecover)
            progress = "100";
    }
    return progress;
}

std::string HandleKits::mediaRecoveryProgress()
{
    std::string progress = "0";
        size_t pToRecover = kits->getShoreEnv()->get_num_pages_vol();
        size_t pRecovered = kits->getShoreEnv()->get_num_restored_pages_vol();

        if (pRecovered > 0)
            progress = std::to_string((static_cast<double>(pRecovered)/static_cast<double>(pToRecover))*100);
        else if (pRecovered >= pToRecover)
            progress = "100";
    return progress;
}

std::string HandleKits::logAnalysisProgress()
{
    std::string progress = "0";
    if (kits->getShoreEnv()->has_log_analysis_finished())
        progress = "100";
    return progress;
}

std::string HandleKits::getStats()
{
    std::string strReturn;
    if (kits->running()) {
        strReturn = "{";
        for (int i = 0; i < countersJson.size(); i++) {
            //countersJson[i][countersJson[i].length()-2] = ']';
            strReturn += (countersJson[i]+ ", ");
            strReturn[strReturn.size()-4] = ']';
        }
        strReturn[strReturn.length()-1] = ' ';
        strReturn[strReturn.length()-2] = '}';
    }
    return strReturn;
};

std::string HandleKits::aggLog()
{
    AggLog agglog;
    agglog.setupOptions();
    int argc=4;
    char* argv[4]={"zapps", "agglog", "-l", "log"};
    po::variables_map varMap;
    po::store(po::parse_command_line(argc,argv,agglog.getOptions()), varMap);
    po::notify(varMap);
    agglog.setOptionValues(varMap);

    agglog.fork();
    agglog.join();
    return agglog.jsonReply();
}

std::string HandleKits::getCounters()
{
    string json;
    if (!kits->running())
        return json;

    AggLog agglog;
    agglog.setupOptions();
    int argc=4;
    char* argv[4]={"zapps", "agglog", "-l", "log"};
    po::variables_map varMap;
    po::store(po::parse_command_line(argc,argv,agglog.getOptions()), varMap);
    po::notify(varMap);
    agglog.setOptionValues(varMap);

    agglog.fork();
    agglog.join();
    json = agglog.jsonReply();
    json[json.length() -2] = ',';

    std::string jsonStats;
    //strReturn = "{";
    for (int i = 0; i < countersJson.size(); i++) {
        //countersJson[i][countersJson[i].length()-2] = ']';
        jsonStats += (countersJson[i]+ ", ");
        jsonStats[jsonStats.size()-4] = ']';
    }
    if (jsonStats.length()>1) {
        jsonStats[jsonStats.length()-1] = ' ';
        jsonStats[jsonStats.length()-2] = '}';
        json +=jsonStats;
    }
    else
        json[json.length()-2] = '}';

    return json;
};

std::string HandleKits::isRunning()
{
    string jsonReply = "{\"isRunning\" : false}";

    if (kits->running()){
        kitsJustStarted = false;
        jsonReply =  "{\"isRunning\" : true}";
    }
    else if (kitsJustStarted)
        jsonReply = "{\"isRunning\" : true}";

    return jsonReply;
};
