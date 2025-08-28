#include "plr.h"
#include "bloom_filter.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <cstdint>
#include <sys/time.h>

// Code modified from https://github.com/RyanMarcus/plr

double get_slope(struct point p1, struct point p2) {
    return (p2.y - p1.y) / (p2.x - p1.x);
}

struct line get_line(struct point p1, struct point p2) {
    double a = get_slope(p1, p2);
    double b = -a * p1.x + p1.y;
    struct line l{.a = a, .b = b};
    return l;
}

struct point get_intersetction(struct line l1, struct line l2) {
    double a = l1.a;
    double b = l2.a;
    double c = l1.b;
    double d = l2.b;
    struct point p {(d - c) / (a - b), (a * d - b * c) / (a - b)};
    return p;
}

bool is_above(struct point pt, struct line l) {
    return pt.y > l.a * pt.x + l.b;
}

bool is_below(struct point pt, struct line l) {
    return pt.y < l.a * pt.x + l.b;
}

struct point get_upper_bound(struct point pt, double gamma) {
    struct point p {pt.x, pt.y + gamma};
    return p;
}

struct point get_lower_bound(struct point pt, double gamma) {
    struct point p {pt.x, pt.y - gamma};
    return p;
}

GreedyPLR::GreedyPLR(double gamma) {
    this->state = NEED2;
    this->gamma = gamma;
}

segment GreedyPLR::process(const struct point& pt) {
    segment s = {0, 0, 0};
    if (this->state == NEED2) {
        this->s0 = pt;
        this->state = NEED1;
    } else if (this->state == NEED1) {
        this->s1 = pt;
        if (setup()) {
            this->state = READY;
        }
    } else if (this->state == READY) {
        s = process__(pt);
    } else {
        // impossible
        std::cout << "ERROR in process" << std::endl;
    }
    this->last_pt = pt;
    return s;
}

bool GreedyPLR::setup() {
    this->rho_lower = get_line(this->s0, get_lower_bound(this->s1, this->gamma));
    if (!this->rho_lower.LineCheck()) {
        return false;
    }
    this->rho_upper = get_line(this->s0, get_upper_bound(this->s1, this->gamma));
    if (!this->rho_upper.LineCheck()) {
        return false;
    }
    this->sint = this->s0;
    // this->sint = get_intersetction(this->rho_upper, this->rho_lower);
    // printf("%lf, %lf, %lf, %lf\n", this->s0.x, this->rho_upper.a, this->sint.x, this->sint.y);
    return true;
}

segment GreedyPLR::current_segment() {
    uint64_t segment_start = this->s0.x;
    double avg_slope = (this->rho_lower.a + this->rho_upper.a) / 2.0;
    double intercept = -avg_slope * this->sint.x + this->sint.y;
    segment s = {segment_start, avg_slope, intercept};
    return s;
}

segment GreedyPLR::process__(struct point pt) {
    if (!(is_above(pt, this->rho_lower) && is_below(pt, this->rho_upper))) {
      // new point out of error bounds
        segment prev_segment = current_segment();
        this->s0 = pt;
        this->state = NEED1;
        return prev_segment;
    }
    struct point s_upper = get_upper_bound(pt, this->gamma);
    struct point s_lower = get_lower_bound(pt, this->gamma);
    if (is_below(s_upper, this->rho_upper)) {
        this->rho_upper = get_line(this->sint, s_upper);
    }
    if (is_above(s_lower, this->rho_lower)) {
        this->rho_lower = get_line(this->sint, s_lower);
    }
    segment s = {0, 0, 0};
    return s;
}

segment GreedyPLR::finish() {
    segment s = {0, 0, 0};
    if (this->state == NEED2) {
        this->state = FIN;
        return s;
    } else if (this->state == NEED1) {
        this->state = FIN;
        s.x = this->s0.x;
        s.k = 0;
        s.b = this->s0.y;
        return s;
    } else if (this->state == READY) {
        this->state = FIN;
        return current_segment();
    } else {
        std::cout << "ERROR in finish" << std::endl;
        return s;
    }
}

PLR::PLR(double gamma) {
    this->gamma = gamma;
    this->filter = create_bloom_filter(64 * 1024 * 1024 / KV_LENGTH + 1);
}

std::vector<segment>& PLR::train(std::vector<uint64_t>& keys, int& average_length) {
    GreedyPLR plr(this->gamma);
    size_t size = keys.size();
    int last_seg_pos = 0, sum_len = 0;
    for (int i = 0; i < size; ++i) {
        add_to_bloom_filter(this->filter, keys[i]);
        segment seg = plr.process(point((double)keys[i], i));
        if (seg.x != 0 || seg.k != 0 || seg.b != 0) {
            sum_len += (i - last_seg_pos);
            last_seg_pos = i;
            this->segments.push_back(seg);
        }
    }
    
    segment last = plr.finish();
    if (last.x != 0 || last.k != 0 || last.b != 0) {
        sum_len += (size - last_seg_pos);
        this->segments.push_back(last);
    }
    average_length = sum_len / this->segments.size();
    return this->segments;
}
