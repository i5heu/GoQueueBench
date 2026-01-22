import json
import math
import statistics
from collections import defaultdict

# Load data from "./test-results.json"
with open("./test-results.json", "r") as f:
    sessions = json.load(f)

# Combine all benchmark tests, attaching system's CPU info and computing total concurrency.
benchmarks = []
for session in sessions:
    # Use simulated_cpu_count if available; otherwise, fall back to num_cpu (or default 8)
    num_cpu = session["system_info"].get("simulated_cpu_count", session["system_info"].get("num_cpu", 8))
    for bench in session["benchmarks"]:
        bench["num_cpu"] = num_cpu
        bench["total_concurrency"] = bench["num_producers"] + bench["num_consumers"]
        benchmarks.append(bench)

# Group tests by implementation and then by CPU count.
impl_cores = defaultdict(lambda: defaultdict(list))
for bench in benchmarks:
    impl = bench["implementation"]
    cpu = bench["num_cpu"]
    impl_cores[impl][cpu].append(bench)

# Calculate baseline, heavy, ratio, etc. for each (impl, cores).
per_impl_cores = defaultdict(lambda: defaultdict(dict))
for impl, groups in impl_cores.items():
    for cpu, tests in groups.items():
        n = len(tests)
        k = max(1, math.ceil(n * 0.05))
        # Baseline: top 5% tests by lowest total concurrency
        tests_sorted_conc = sorted(tests, key=lambda x: x["total_concurrency"])
        baseline_group = tests_sorted_conc[:k]
        baseline_vals = [t["throughput_msgs_sec"] for t in baseline_group]
        baseline_avg = sum(baseline_vals) / len(baseline_vals)

        # Worst-case: bottom 5% tests by throughput
        tests_sorted_throughput = sorted(tests, key=lambda x: x["throughput_msgs_sec"])
        heavy_group = tests_sorted_throughput[:k]
        heavy_vals = [t["throughput_msgs_sec"] for t in heavy_group]
        heavy_avg = sum(heavy_vals) / len(heavy_vals)
        
        # Calculate average throughput of all tests
        avg_throughput = sum(t["throughput_msgs_sec"] for t in tests) / len(tests) if tests else 0

        ratio = heavy_avg / baseline_avg if baseline_avg else 0
        all_vals = [t["throughput_msgs_sec"] for t in tests]
        group_std = statistics.stdev(all_vals) if len(all_vals) > 1 else 0
        group_avg = sum(all_vals) / len(all_vals)
        uncertainty = group_std / group_avg if group_avg > 0 else 0

        per_impl_cores[impl][cpu] = {
            "baseline": baseline_avg,
            "heavy": heavy_avg,
            "avg_throughput": avg_throughput,
            "ratio": ratio,
            "uncertainty": uncertainty,
            "tests": n
        }

# --- NEW: Compute Homogeneity Factor ---
homogeneity_factors = {}  # overall homogeneity for each implementation

for impl, groups in impl_cores.items():
    group_factors = []
    for cpu, tests in groups.items():
        # Group tests by total_concurrency.
        conc_dict = defaultdict(list)
        for test in tests:
            conc_dict[test["total_concurrency"]].append(test["throughput_msgs_sec"])
        # Compute average throughput per concurrency level.
        avg_by_conc = {conc: sum(v)/len(v) for conc, v in conc_dict.items()}
        if not avg_by_conc:
            continue
        # Sort concurrency levels.
        sorted_concs = sorted(avg_by_conc.keys())
        if len(sorted_concs) < 2:
            group_factors.append(1.0)
            continue
        # Compute total "wiggle" as the sum of absolute differences in log throughput.
        wiggle_sum = 0.0
        for i in range(len(sorted_concs) - 1):
            t_curr = avg_by_conc[sorted_concs[i]]
            t_next = avg_by_conc[sorted_concs[i+1]]
            if t_curr > 0 and t_next > 0:
                diff = abs(math.log(t_next) - math.log(t_curr))
                wiggle_sum += diff
        # Use a lower alpha to make the penalty more forgiving.
        alpha = 0.2
        group_factor = math.exp(-alpha * wiggle_sum)
        group_factors.append(group_factor)
    if group_factors:
        overall_homogeneity = math.exp(sum(math.log(x) for x in group_factors) / len(group_factors))
    else:
        overall_homogeneity = 1.0
    homogeneity_factors[impl] = overall_homogeneity

# --- Compute Overall Metrics per Implementation ---
overall_metrics = {}
for impl, groups in per_impl_cores.items():
    baseline_list, heavy_list, avg_list, ratio_list, uncertainties = [], [], [], [], []
    total_tests = 0
    for cpu, metrics in groups.items():
        if metrics["baseline"] > 0 and metrics["heavy"] > 0:
            baseline_list.append(metrics["baseline"])
            heavy_list.append(metrics["heavy"])
            ratio_list.append(metrics["ratio"])
            avg_list.append(metrics["avg_throughput"])
            uncertainties.append(metrics["uncertainty"])
        total_tests += metrics["tests"]
    if baseline_list:
        overall_baseline = math.exp(sum(math.log(x) for x in baseline_list) / len(baseline_list))
        overall_heavy = math.exp(sum(math.log(x) for x in heavy_list) / len(heavy_list))
        overall_avg = math.exp(sum(math.log(x) for x in avg_list) / len(avg_list))
        # Possibly use a harmonic-mean approach for ratio:
        if all(x > 0 for x in ratio_list):
            overall_ratio = len(ratio_list)*1.5 / sum(1/x for x in ratio_list)
        else:
            overall_ratio = 0

        # Logarithmic mapping for a multiplier that dampens the effect of overall_ratio
        ratio_multiplier = 0.5 + 1.1 * math.log(1 + overall_ratio**0.9)

        # Use the new wiggle-based homogeneity factor
        overall_homogeneity = homogeneity_factors.get(impl, 1)
        overall_score = ((overall_baseline * 0.5) + (overall_heavy * 0.5) + (overall_avg * 0.4)) * ratio_multiplier * overall_homogeneity

        overall_uncertainty = max(uncertainties) if uncertainties else 0
    else:
        overall_baseline = overall_heavy = overall_ratio = overall_score = overall_uncertainty = 0
        overall_homogeneity = 1
        ratio_multiplier = 0

    overall_metrics[impl] = {
        "overall_score": overall_score,
        "geo_baseline": overall_baseline,
        "geo_heavy": overall_heavy,
        "geo_avg": overall_avg,
        "stability_ratio": overall_ratio,
        "homogeneity_factor": overall_homogeneity,
        "uncertainty": overall_uncertainty,
        "total_tests": total_tests
    }

# Utility: Bold the value if it equals the best.
def bold_if_best(value, best, fmt=".2f", reverse=False):
    formatted = f"{value:{fmt}}"
    if reverse:
        return f"**{formatted}**" if value == best else formatted
    else:
        return f"**{formatted}**" if value == best else formatted

# Determine best values for each column in the Overall Summary.
best_overall_score    = max(m["overall_score"] for m in overall_metrics.values()) if overall_metrics else 0
best_geo_baseline     = max(m["geo_baseline"] for m in overall_metrics.values()) if overall_metrics else 0
best_geo_heavy        = max(m["geo_heavy"] for m in overall_metrics.values()) if overall_metrics else 0
best_geo_avg          = max(m["geo_avg"] for m in overall_metrics.values()) if overall_metrics else 0
best_stability_ratio  = max(m["stability_ratio"] for m in overall_metrics.values()) if overall_metrics else 0
best_homogeneity_factor = max(m["homogeneity_factor"] for m in overall_metrics.values()) if overall_metrics else 0
best_uncertainty      = min(m["uncertainty"] for m in overall_metrics.values()) if overall_metrics else 0  # lower is better

# Print Overall Summary table sorted by Overall Score (highest first).
print("## Overall Summary")
print("| Implementation              | Overall Score | Throughput Light Load | Throughput Heavy Load | Throughput Average | Stability Ratio | Homogeneity Factor | Uncertainty | Total Tests |")
print("|-----------------------------|---------------|-----------------------|-----------------------|--------------------|-----------------|--------------------|-------------|-------------|")
for impl, m in sorted(overall_metrics.items(), key=lambda item: item[1]["overall_score"], reverse=True):
    overall_score_str   = bold_if_best(m["overall_score"], best_overall_score, fmt=".0f")  # higher is best
    geo_baseline_str    = bold_if_best(m["geo_baseline"], best_geo_baseline, fmt=".0f")  # higher is best
    geo_heavy_str       = bold_if_best(m["geo_heavy"], best_geo_heavy, fmt=".0f")  # higher is best
    geo_avg_str         = bold_if_best(m["geo_avg"], best_geo_avg, fmt=".0f")  # higher is best
    stability_ratio_str = bold_if_best(m["stability_ratio"], best_stability_ratio)  # higher is best
    homogeneity_factor_str = bold_if_best(m["homogeneity_factor"], best_homogeneity_factor)  # higher is best
    uncertainty_str     = bold_if_best(m["uncertainty"], best_uncertainty, fmt=".2f", reverse=True)  # lower is best
    print(f"| {impl:<27} | {overall_score_str:>13} | {geo_baseline_str:>21} | {geo_heavy_str:>21} | {geo_avg_str:>18} | {stability_ratio_str:>15} | {homogeneity_factor_str:>18} | {uncertainty_str:>11} | {m['total_tests']:>11} |")

# --- Compute Local Homogeneity Factor per Cores Group ---
local_homogeneity = defaultdict(dict)
for impl, groups in impl_cores.items():
    for cpu, tests in groups.items():
        conc_dict = defaultdict(list)
        for test in tests:
            conc_dict[test["total_concurrency"]].append(test["throughput_msgs_sec"])
        avg_by_conc = {conc: sum(v)/len(v) for conc, v in conc_dict.items()}
        sorted_concs = sorted(avg_by_conc.keys())
        if len(sorted_concs) < 2:
            local_homogeneity[impl][cpu] = 1.0
            continue
        wiggle_sum = 0.0
        for i in range(len(sorted_concs)-1):
            t_curr = avg_by_conc[sorted_concs[i]]
            t_next = avg_by_conc[sorted_concs[i+1]]
            if t_curr > 0 and t_next > 0:
                diff = abs(math.log(t_next) - math.log(t_curr))
                wiggle_sum += diff
        # Use a tuning parameter alpha; lower alpha makes the penalty more forgiving.
        alpha = 0.2
        group_factor = math.exp(-alpha * wiggle_sum)
        local_homogeneity[impl][cpu] = group_factor

# --- Build the Matrix of Local Scores by Cores Group ---
matrix = defaultdict(dict)
for impl, groups in per_impl_cores.items():
    for cpu, metrics in groups.items():
        # Incorporate the local homogeneity factor into the local score.
        # Without homogeneity, local_score = sqrt(baseline * heavy).
        # Now, we multiply by the local factor for this cores group.
        lh = local_homogeneity[impl].get(cpu, 1.0)
        local_score = math.sqrt(metrics["baseline"] * metrics["heavy"]) * lh
        matrix[impl][cpu] = local_score

# Determine all Cores groups.
cores_groups = set()
for impl, groups in matrix.items():
    cores_groups.update(groups.keys())
cores_groups = sorted(cores_groups)

# For each Cores group, determine the best (highest) local score.
best_scores = {}
for cpu in cores_groups:
    best = 0
    for impl in matrix:
        if cpu in matrix[impl]:
            best = max(best, matrix[impl][cpu])
    best_scores[cpu] = best

print("\n## Local Scores by Cores Group")
# Build header using fixed widths.
header = "| {impl:<27} | ".format(impl="Implementation")
for cpu in cores_groups:
    header += f"Score {cpu}Cores".rjust(14) + " | "
print(header)

separator = "|" + "-"*29 + "|" + "|".join(["-"*16 for _ in cores_groups]) + "|"
print(separator)

for impl in sorted(matrix.keys()):
    row = f"| {impl:<27} |"
    for cpu in cores_groups:
        if cpu in matrix[impl]:
            val = matrix[impl][cpu]
            cell = f"**{val:.0f}**" if val == best_scores[cpu] else f"{val:.0f}"
        else:
            cell = "N/A"
        row += f" {cell:>14} |"
    print(row)

# --- Build the Matrix of Local Scores by Concurrency Group ---
impl_concurrency = defaultdict(lambda: defaultdict(list))
for bench in benchmarks:
    impl = bench["implementation"]
    conc = bench["total_concurrency"]
    impl_concurrency[impl][conc].append(bench)

local_scores_conc = defaultdict(dict)
for impl, groups in impl_concurrency.items():
    for conc, tests in groups.items():
        n = len(tests)
        k = max(1, math.ceil(n * 0.05))
        # Baseline: best throughput tests (sorted descending)
        tests_sorted_desc = sorted(tests, key=lambda x: x["throughput_msgs_sec"], reverse=True)
        baseline_group = tests_sorted_desc[:k]
        baseline_vals = [t["throughput_msgs_sec"] for t in baseline_group]
        baseline_avg = sum(baseline_vals) / len(baseline_vals)
        # Worst-case: worst throughput tests (sorted ascending)
        tests_sorted_asc = sorted(tests, key=lambda x: x["throughput_msgs_sec"])
        heavy_group = tests_sorted_asc[:k]
        heavy_vals = [t["throughput_msgs_sec"] for t in heavy_group]
        heavy_avg = sum(heavy_vals) / len(heavy_vals)
        # For concurrency groups we use a homogeneity factor of 1 (since all tests share the same concurrency)
        local_score = math.sqrt(baseline_avg * heavy_avg)
        local_scores_conc[impl][conc] = local_score

# Determine all Concurrency groups.
concurrency_groups = set()
for impl, groups in local_scores_conc.items():
    concurrency_groups.update(groups.keys())
concurrency_groups = sorted(concurrency_groups)

# For each Concurrency group, determine the best (highest) local score.
best_scores_conc = {}
for conc in concurrency_groups:
    best = 0
    for impl in local_scores_conc:
        if conc in local_scores_conc[impl]:
            best = max(best, local_scores_conc[impl][conc])
    best_scores_conc[conc] = best

print("\n## Local Scores by Concurrency Group")
# Build header using fixed widths.
header = "| {impl:<27} | ".format(impl="Implementation")
for conc in concurrency_groups:
    header += f"Score {conc}Conc".rjust(16) + " | "
print(header)

separator = "|" + "-"*29 + "|" + "|".join(["-"*18 for _ in concurrency_groups]) + "|"
print(separator)

for impl in sorted(local_scores_conc.keys()):
    row = f"| {impl:<27} |"
    for conc in concurrency_groups:
        if conc in local_scores_conc[impl]:
            val = local_scores_conc[impl][conc]
            cell = f"**{val:.0f}**" if val == best_scores_conc[conc] else f"{val:.0f}"
        else:
            cell = "N/A"
        row += f" {cell:>16} |"
    print(row)
