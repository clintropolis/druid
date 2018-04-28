#include <iostream>
#include <fstream>
#include <cstdlib>
#include <ctime>

#include "codecfactory.h"
using namespace std;
using namespace FastPForLib;

int main()
{
  IntegerCODEC &codec = *CODECFactory::getFromName("simdfastpfor256");
  size_t N = 10000;
  std::vector<uint32_t> mydata(N);

  srand (time(NULL));

  for (uint32_t i = 0; i < N; i++)
    mydata[i] = rand() % 100;

  std::vector<uint32_t> compressed_output(N + 1024);
  size_t compressedsize = compressed_output.size();
  codec.encodeArray(mydata.data(), mydata.size(), compressed_output.data(),
                    compressedsize);
  ofstream out("numbers.bin", ios::out | ios::binary);
  if(!out) {
    cout << "Cannot open file.";
    return 1;
   }

  out.write((char *)compressed_output.data(), N * sizeof(uint32_t));

  out.close();

  cout << "length " << compressedsize << "\n";
  return 0;
}