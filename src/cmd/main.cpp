#include "base/command.h"

int main(int argc, char ** argv)
{
    Command::init();

    Command *cmd;
    cmd = Command::parse(argc, argv);
    if (!cmd) {
        return 1;
    }

    cmd->fork();
    cmd->join();

    return EXIT_SUCCESS;
}
