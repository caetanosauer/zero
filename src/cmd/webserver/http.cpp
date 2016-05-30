#include "webserver.h"
#include <time.h>
http_headers::http_headers()
{
}

std::string http_headers::get_response()
{
  std::stringstream ssOut;
  if(url == "/favicon.ico")
  {
//     ssOut << "HTTP/1.1 200 OK" << std::endl;
//     ssOut << "content-type: image/vnd.microsoft.icon" << std::endl;
//     ssOut << std::endl;
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
  if(headers.find("content-length") != headers.end())
  {
     std::stringstream ssLength(headers.find("content-length")->second);
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
  
};

void session::read_body(std::shared_ptr<session> pThis)
{
  int nbuffer = 1000;
  std::shared_ptr<std::vector<char>> bufptr = std::make_shared<std::vector<char>>(nbuffer);
  asio::async_read(pThis->socket, boost::asio::buffer(*bufptr, nbuffer), [pThis](const boost::system::error_code& e, std::size_t s)
  {
  });
};
   
void session::read_next_line(std::shared_ptr<session> pThis)
{
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis](const boost::system::error_code& e, std::size_t s)
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
           std::shared_ptr<std::string> str = std::make_shared<std::string>(pThis->headers.get_response());
           asio::async_write(pThis->socket, boost::asio::buffer(str->c_str(), str->length()), [pThis, str](const boost::system::error_code& e, std::size_t s)
           {
              //std::cout << "done" << std::endl;
           });
        }
        else
        {
           pThis->read_body(pThis);
        }
     }
     else
     {
        pThis->read_next_line(pThis);
     }
  });
};

void session::read_first_line(std::shared_ptr<session> pThis)
{
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis](const boost::system::error_code& e, std::size_t s)
  {
     std::string line, ignore;
     std::istream stream {&pThis->buff};
     std::getline(stream, line, '\r');
     std::getline(stream, ignore, '\n');
     pThis->headers.on_read_request_line(line);
     pThis->read_next_line(pThis);
  });
};
   
void session::interact(std::shared_ptr<session> pThis)
{
  read_first_line(pThis);
};


void Webserver::setupOptions()
{
    boost::program_options::options_description opt("Webserver Options");

    options.add(opt);
}

void accept_and_run(ip::tcp::acceptor& acceptor, io_service& io_service)
{
   std::shared_ptr<session> sesh = std::make_shared<session>(io_service);
   acceptor.async_accept(sesh->socket, [sesh, &acceptor, &io_service](const boost::system::error_code& accept_error)
   {
      accept_and_run(acceptor, io_service);
      if(!accept_error)
      {
         session::interact(sesh);
      }
   });
};

void Webserver::run()
{
    io_service io_service;
    ip::tcp::endpoint endpoint{ip::tcp::v4(), 8080};
    ip::tcp::acceptor acceptor{io_service, endpoint};
    acceptor.listen();
    accept_and_run(acceptor, io_service);

    io_service.run();

}
