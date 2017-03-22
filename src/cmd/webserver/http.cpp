#include "http.h"
#include <algorithm>


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
     kits->runKits(options);
  }
  else if(url == "/getstats")
  {
     string json = kits->getStats(false); // TODO pass from http arg
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
      std::stringstream content;
      content << "{\"redoProgress\":" << kits->redoProgress();
      content << ", \"logAnalysisProgress\":" << kits->logAnalysisProgress();
      content << ", \"undoProgress\":" << kits->undoProgress();
      content << ", \"restoreProgress\":" << kits->mediaRecoveryProgress();
      content << "}" << std::endl;

      ssOut << "HTTP/1.1 200 OK" << std::endl;
      ssOut << "Access-Control-Allow-Origin: *" << std::endl;
      ssOut << "content-type: application/json" << std::endl;
      ssOut << "content-length: " << content.str().length() << std::endl;
      ssOut << std::endl;
      ssOut << content.str();
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
   // std::cout << "request for resource: " << url << std::endl;
};

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

            pThis->headers.options << body;
            std::shared_ptr<std::string> str = std::make_shared<std::string>(pThis->headers.get_response(kits));
            asio::async_write(pThis->socket, boost::asio::buffer(str->c_str(), str->length()), [pThis, str](const boost::system::error_code& /*e*/, std::size_t /*s*/)
            {
               //std::cout << "done" << std::endl;
            });
        });

}

void session::read_next_line(std::shared_ptr<session> pThis, HandleKits* kits)
{
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis, kits](const boost::system::error_code& /*e*/, std::size_t /*s*/)
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
           asio::async_write(pThis->socket, boost::asio::buffer(str->c_str(), str->length()), [pThis, str](const boost::system::error_code& /*e*/, std::size_t /*s*/)
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
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis, kits](const boost::system::error_code& /*e*/, std::size_t /*s*/)
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


HandleKits::HandleKits()
    : kits(nullptr)
{
}

int HandleKits::runKits(std::stringstream &kits_options)
{
    if (kits) {
        std::cout << "kits running!!!!" << std::endl;
        return 1;
    }

    kits = new KitsCommand();
    kits->setupOptions();

    po::variables_map vm;
    po::store(po::parse_config_file(kits_options, kits->getOptions()), vm);
    po::notify(vm);
    kits->setOptionValues(vm);

    kits->fork();

    if (!statsThread) {
        statsThread.reset(new std::thread(&HandleKits::computeStats, this));
    }

    return 0;
};

void HandleKits::crash()
{
    if (kits && kits->running()) {
        kits->crashFilthy();
        kits->join();
        delete kits;
        kits = nullptr;
    }
}

void HandleKits::mediaFailure()
{
    if (kits && kits->running()) {
        kits->mediaFailure(0);
    }
}

void HandleKits::singlePageFailure()
{
    if (kits && kits->running()) {
        kits->randomRootPageFailure();
    }
}

std::string HandleKits::redoProgress()
{
    std::string progress = "0";
    if (kits && kits->getShoreEnv()->has_log_analysis_finished()) {
        size_t total = kits->getShoreEnv()->get_total_pages_to_recover();
        size_t dirty = kits->getShoreEnv()->get_dirty_page_count();

        if (total > 0 && dirty > 0) {
            progress = std::to_string(((total - dirty) * 100) / total);
        }
        else {
            progress = "100";
        }
    }
    return progress;
}

std::string HandleKits::undoProgress()
{
    std::string progress = "0";
    if (kits && kits->getShoreEnv()->has_log_analysis_finished()) {
        size_t total = xct_t::num_active_xcts();
        size_t active = xct_t::get_loser_count();

        if (total > 0 && active > 0) {
            progress = std::to_string(((total - active) * 100) / total);
        }
        else {
            progress = "100";
        }
    }
    return progress;
}

std::string HandleKits::mediaRecoveryProgress()
{
    std::string progress = "0";
    if (kits && kits->running()) {
        size_t total = kits->getShoreEnv()->get_total_pages_to_restore();
        size_t restored = kits->getShoreEnv()->get_num_restored_pages();

        if (total > 0 && restored > 0) {
            progress = std::to_string((restored * 100) / total);
        }
        else if (total == 0) {
            progress = "0";
        }
        else {
            progress = "100";
        }
    }
    return progress;
}

std::string HandleKits::logAnalysisProgress()
{
    // TODO
    std::string progress = "0";
    if (kits && kits->getShoreEnv()->has_log_analysis_finished())
        progress = "100";
    return progress;
}

void HandleKits::computeStats()
{
    using namespace std::chrono_literals;

    while (true) {
        std::this_thread::sleep_for(1s);

        std::unique_lock<std::mutex> lck {stats_mutex};

        stats.emplace_back();
        auto& st = stats[stats.size() - 1];
        st.fill(0);

        stats_delta.emplace_back();
        auto& st_delta = stats_delta[stats_delta.size() - 1];
        st_delta.fill(0);

        // if (kits && kits->running()) {
            ss_m::gather_stats(st);
        // }

        if (stats.size() > 1) {
            auto& st2 = stats[stats.size() - 2];
            // if kits not running, just copy last values
            if (kits && !kits->running()) {
                for (size_t i = 0; i < st.size(); i++) {
                    st[i] = st2[i];
                }
            }

            for (size_t i = 0; i < st.size(); i++) {
                if (st[i] > st2[i]) {
                    st_delta[i] = st[i] - st2[i];
                }
                else { st_delta[i] = 0; }
            }
        }

        // CS TODO: fix this
        const size_t moving_avg = 0;
        if (moving_avg > 0) {
            size_t m = std::min(moving_avg, stats_delta.size());
            for (size_t i = 1; i < m; i++) {
                for (size_t j = 0; j < st.size(); j++) {
                    auto& st_tmp = stats_delta[(stats_delta.size() - 1) - m];
                    st_delta[j] += st_tmp[j];
                }
            }
            for (size_t j = 0; j < st.size(); j++) {
                st_delta[j] /= m;
            }
        }
    }
}

std::string HandleKits::getStats(bool cumulative)
{
    std::unique_lock<std::mutex> lck {stats_mutex};

    auto& s = cumulative ? stats : stats_delta;

    if (s.size() < 1) { return ""; }

    std::stringstream strReturn;
    strReturn << "{" << std::endl;
    auto cnt = s[0].size();
    for (size_t i = 0; i < cnt; i++) {
        strReturn << "\"" << get_stat_name(static_cast<sm_stat_id>(i)) << "\" : [";
        auto max = s.size();
        for (size_t j = 0; j < max; j++) {
            strReturn << s[j][i];
            if (j < max - 1 ) {
                strReturn << ", ";
            }
        }
        strReturn << "]";
        if (i < cnt-1) {
            strReturn << ", ";
        }
        strReturn << std::endl;
    }
    strReturn << "}";

    return strReturn.str();
};

std::string HandleKits::isRunning()
{
    string jsonReply = "{\"isRunning\" : \"no\"}";

    if (kits && kits->running())
        jsonReply =  "{\"isRunning\" : \"yes\"}";
    else if (kits && !kits->running())
        jsonReply =  "{\"isRunning\" : \"initializing\"}";

    return jsonReply;
};
