#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef struct {
    int a;
    bool c;
    float b;
} def_T;

typedef def_T oth_T;

typedef oth_T pmi_T[32];

typedef struct {

    struct {
        bool m;
        double z;
    } q;

    pmi_T t;

} main_T;

main_T blob = {0};

int main(int argc, char * argv[]) {

    memset(& blob, 0, sizeof(main_T));

    return EXIT_SUCCESS;

}
