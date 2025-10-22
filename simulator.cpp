#include <iostream>
#include <fstream>
#include <string>
#include <sstream>

class VMMSimulator {
private:
    int acc; // accumulator register 

public:
    VMMSimulator() : acc(0) {}

    void run(const std::string& filename){
        std::ifstream file(filename);
        std::string line;

        if(!file.is_open()){
            std::cerr << "Error: Could not open file" << std::endl;
        }

        while(std::getline(file, line)){

            if(line.empty()) continue;

            // parse instruction
            std::istringstream iss(line);
            std::string instruction;
            iss >> instruction;

            if (instruction == "add"){
                int value;
                iss >> value;
                std::cout << "[Guest] Executing: add" << value << std::endl;
                acc += value;
            }
            else if (instruction == "print"){
                std::cout << "[Guest] Executing: print" << std::endl;
                std::cout << 'Accumalator value: ' << acc << std::endl;
            }
            else if (instruction == "scan_disk"){
                std::cout << "[VMM] Trapped privileged instruction 'scan_disk', emulating..." << std::endl;
                // Emulate disk scanning
            }
            else if(instruction == "halt"){
                std::cout << "[VMM] Trapped privileged instruction 'halt'. Halting guest." << std::endl;
                break; //stop execution
            }
            else {
                std::cerr << "Error: Unknown instrucion " << instruction << std::endl;
            }
        }

        file.close();
    }
};


int main() {
    VMMSimulator simulator;
    simulator.run("guest_program.txt");
    return 0;
}