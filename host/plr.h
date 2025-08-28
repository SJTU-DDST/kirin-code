#include <cmath>
#include <string>
#include <vector>
#include <deque>
#include "kv.h"
#include "bloom_filter.h"
// Code modified from https://github.com/RyanMarcus/plr

#define NEED1 0
#define NEED2 1
#define READY 2
#define FIN 3
struct point {
    double x;
    double y;
    point() = default;
    point(double x, double y) : x(x), y(y) {}
};

struct line {
    double a;
    double b;

    bool LineCheck() {
        if (!std::isnan(a) && !std::isnan(b)) {
            return true;
        }
        return false;
    }
};

typedef struct 
{
    uint64_t x;
    double k;
    double b;
}segment;

double get_slope(struct point p1, struct point p2);
struct line get_line(struct point p1, struct point p2);
struct point get_intersetction(struct line l1, struct line l2);

bool is_above(struct point pt, struct line l);
bool is_below(struct point pt, struct line l);

struct point get_upper_bound(struct point pt, double gamma);
struct point get_lower_bound(struct point pt, double gamma);


class GreedyPLR {
private:
    int32_t state;
    double gamma;
    struct point last_pt;
    struct point s0;
    struct point s1;
    struct line rho_lower;
    struct line rho_upper;
    struct point sint;

    bool setup();
    segment current_segment();
    segment process__(struct point pt);

public:
    GreedyPLR(double gamma);
    segment process(const struct point& pt);
    segment finish();
};

class PLR {
private:
    double gamma;

public:
    std::vector<segment> segments;
    PLR(double gamma);
    std::vector<segment>& train(std::vector<uint64_t>& keys, int& average_length);
    BloomFilter* filter;
//    std::vector<double> predict(std::vector<double> xx);
//    double mae(std::vector<double> y_true, std::vector<double> y_pred);
};
