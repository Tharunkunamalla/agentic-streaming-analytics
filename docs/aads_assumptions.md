# AADS Engineering Assumptions and Specifications

This document explicitly details all engineering decisions and parameter assumptions made in our implementation of the **AADS (Autonomous Anomaly Detection for Streaming Data)** baseline (*Knowledge-Based Systems*, Vol. 284, 2024).

The AADS paper establishes a core three-stage framework:
1. **Data-density-based identification** of potentially anomalous samples.
2. **Online clustering** using the evolving autonomous data partitioning (ADP) approach.
3. **Identification of true anomalies** from minor clusters.

Where mathematical specifications or streaming thresholds require concrete engineering instantiation for production cloud time-series, the decisions below were adopted. All assumptions are completely configurable via `AADSConfig`.

---

## 1. Density Calculation (Stage 1)

### Engineering Assumption 1.1: Empirical Data Analytics (EDA) Cauchy Density Form
- **Paper Reference:** The paper specifies nonparametric data density based on global mean and variance, derived from Gu & Angelov's Empirical Data Analytics framework.
- **Implementation:**
  $$\mu_k = (1 - \alpha) \mu_{k-1} + \alpha x_k$$
  $$\sigma_k^2 = (1 - \alpha) \sigma_{k-1}^2 + \alpha (x_k - \mu_k)^2$$
  $$D(x_k) = \frac{1}{1 + \frac{(x_k - \mu_k)^2}{\sigma_k^2 + \epsilon}}$$
- **Reason:** Standard streaming recursion allows constant $O(1)$ time and memory updates per incoming point without storing unbounded historical series.
- **Sensitivity:** Tested with window size $W \in [30, 50, 100, 200]$ and exponential decay $\alpha \in [0.01, 0.05, 0.1]$.

### Engineering Assumption 1.2: Potential Anomaly Filtering Threshold
- **Paper Reference:** Samples with density significantly lower than ensemble density are treated as potential anomalies.
- **Implementation:** A point $x_k$ is passed to Stage 2 if its normalized distance $\frac{|x_k - \mu_k|}{\sigma_k} \ge \theta_{density}$ (default $\theta_{density} = 2.5$) or its density $D(x_k) \le D_{threshold}$.
- **Reason:** In high-velocity streaming, passing 100% of nominal points to cluster partitioning degrades throughput. Filtering nominal high-density points protects clustering stability.
- **Sensitivity:** Tested with threshold $\theta_{density} \in [2.0, 2.5, 3.0, 3.5]$.

---

## 2. Autonomous Data Partitioning (Stage 2)

### Engineering Assumption 2.1: Micro-Cluster Influence Radius
- **Paper Reference:** Autonomous data partitioning groups points into local clusters without predefining cluster count $K$.
- **Implementation:** A sample $x_k$ is assigned to the nearest cluster $C_j$ if Euclidean distance $|x_k - c_j| \le r_j$. The cluster radius is initialized as $r_0 = \max(\sigma_k \cdot \gamma, \epsilon)$ (default $\gamma = 0.5$).
- **Reason:** Self-adjusting radius based on local standard deviation ensures clustering scales dynamically across diverse KPI magnitudes.
- **Sensitivity:** Tested with radius scaling $\gamma \in [0.25, 0.5, 1.0]$.

### Engineering Assumption 2.2: Cluster Support and Center Updates
- **Paper Reference:** When a sample merges into a cluster, its center and support update incrementally.
- **Implementation:**
  $$c_j \leftarrow \frac{S_j c_j + x_k}{S_j + 1}, \quad S_j \leftarrow S_j + 1$$
  If no existing cluster is within radius $r_j$, a new micro-cluster is spawned with center $c_{new} = x_k, S_{new} = 1$.
- **Reason:** Standard online centroid update guarantees $O(1)$ insertion.
- **Sensitivity:** Tested under single-stream and multi-stream scenarios.

---

## 3. Minor Cluster Identification (Stage 3)

### Engineering Assumption 3.1: Minor Cluster Threshold ($\tau_{minor}$)
- **Paper Reference:** True anomalies are isolated from "minor clusters" that contain infrequent samples compared to dominant nominal clusters.
- **Implementation:** A cluster $C_j$ is classified as a minor cluster if:
  $$S_j \le \max(1, \lfloor \tau_{ratio} \cdot W \rfloor) \quad \text{or} \quad S_j \le S_{max\_minor}$$
  Default: $S_{max\_minor} = 3$ or cluster support ratio $< 5\%$ of active window.
- **Reason:** Single noise spikes and short anomaly bursts typically form clusters of size 1 to 3, whereas legitimate regime changes quickly accumulate support $> 5$.
- **Sensitivity:** Tested with $S_{max\_minor} \in [1, 2, 3, 5, 8]$.

### Engineering Assumption 3.2: Cluster Aging and Pruning (Streaming Bounded Memory)
- **Paper Reference:** The paper addresses non-stationary streaming data.
- **Implementation:** Clusters not updated for more than $W_{decay}$ steps (default $W_{decay} = 500$) have their support decayed or pruned.
- **Reason:** Unbounded cluster accumulation causes memory leaks in long-running 24/7 streaming engines.
- **Sensitivity:** Tested with decay horizon $W_{decay} \in [200, 500, 1000]$.

---

## Summary of Default Configurable Parameters

| Parameter | Default | Config Key | Description |
|---|---|---|---|
| Density Window | 100 | `density_window_size` | Size of sliding baseline for $\mu, \sigma$ |
| Density Threshold | 2.5 | `density_z_threshold` | Z-score threshold for Stage 1 filter |
| Cluster Radius Factor | 0.5 | `cluster_radius_factor` | Scaling factor $\gamma$ for cluster radius |
| Max Minor Cluster Size | 3 | `max_minor_cluster_size` | Maximum members for Stage 3 anomaly classification |
| Cluster Age Decay | 500 | `cluster_max_idle_steps` | Steps before inactive micro-clusters are pruned |
