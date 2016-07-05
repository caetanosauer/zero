#include <boost/asio.hpp>
#include <memory>
#include "http.h"


void accept_and_run(ip::tcp::acceptor& acceptor, io_service& io_service, HandleKits &kits)
{
   std::shared_ptr<session> sesh = std::make_shared<session>(io_service);
   acceptor.async_accept(sesh->socket, [sesh, &acceptor, &io_service, &kits](const boost::system::error_code& accept_error)
   {
      accept_and_run(acceptor, io_service, kits);
      if(!accept_error)
      {
         session::interact(sesh, kits);
      }
   });
};

int main(int argc, char ** argv)
{
    HandleKits kits;
    io_service io_service;
    ip::tcp::endpoint endpoint{ip::tcp::v4(), 8080};
    ip::tcp::acceptor acceptor{io_service, endpoint};
    acceptor.listen();
    accept_and_run(acceptor, io_service, kits);

    io_service.run();
    return  EXIT_SUCCESS;
}
