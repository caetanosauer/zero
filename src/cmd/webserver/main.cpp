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
    short unsigned int tcpPort = 8080;
    if (argc>=2)
    {
        tcpPort = (short unsigned int) std::stoi(argv[1]);
        if (tcpPort > 65500)
        {
            cout << "ERROR: Define a valid port" << endl;
            return EXIT_FAILURE;
        }
    }
    cout << "Web server is running on port " << tcpPort << endl;
    HandleKits kits;
    io_service io_service;
    ip::tcp::endpoint endpoint{ip::tcp::v4(), tcpPort};
    ip::tcp::acceptor acceptor{io_service, endpoint};
    acceptor.listen();
    accept_and_run(acceptor, io_service, kits);

    io_service.run();
    return  EXIT_SUCCESS;
}
