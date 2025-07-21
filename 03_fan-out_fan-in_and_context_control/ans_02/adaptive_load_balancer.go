package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LoadBalancerNodeは負荷分散ノードです
type LoadBalancerNode struct {
	ID              string
	Address         string
	Weight          int
	CurrentLoad     int64
	MaxCapacity     int64
	Health          NodeHealth
	ResponseTime    time.Duration
	SuccessRate     float64
	LastHealthCheck time.Time
	Metadata        map[string]interface{}
}

// NodeHealthはノード健全性状態です
type NodeHealth int

const (
	HealthUnknown NodeHealth = iota
	HealthHealthy
	HealthDegraded
	HealthUnhealthy
)

// RequestTaskは処理要求タスクです
type RequestTask struct {
	ID            int64
	RequestType   string
	Payload       []byte
	Priority      int
	Timeout       time.Duration
	RequiredNodes int
	Metadata      map[string]interface{}
	SubmittedAt   time.Time
}

// ProcessingResultは処理結果です
type ProcessingResult struct {
	TaskID      int64
	NodeID      string
	Success     bool
	Result      interface{}
	Error       error
	ProcessTime time.Duration
	QueueTime   time.Duration
	CompletedAt time.Time
}

// AdaptiveLoadBalancerは適応型負荷分散システムです
type AdaptiveLoadBalancer struct {
	// ノード管理
	nodes      map[string]*LoadBalancerNode
	nodesMutex sync.RWMutex

	// 負荷分散戦略
	strategy LoadBalancingStrategy

	// Fan-out制御
	fanOutFactor  int
	maxConcurrent int

	// チャネル
	requestQueue chan RequestTask
	resultQueue  chan ProcessingResult

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats *LoadBalancerStats

	// ヘルスチェック
	healthChecker *HealthChecker

	// 制御フラグ
	isRunning int32
	startTime time.Time
}

// LoadBalancingStrategyは負荷分散戦略です
type LoadBalancingStrategy int

const (
	StrategyRoundRobin LoadBalancingStrategy = iota
	StrategyWeightedRoundRobin
	StrategyLeastConnections
	StrategyLeastResponseTime
	StrategyAdaptive
)

// LoadBalancerStatsは負荷分散統計です
type LoadBalancerStats struct {
	mu                  sync.RWMutex
	totalRequests       int64
	totalSuccessful     int64
	totalErrors         int64
	totalFanOutRequests int64
	averageResponseTime time.Duration
	nodeStats           map[string]*NodeStats
	strategyStats       map[LoadBalancingStrategy]*StrategyStats
	startTime           time.Time
}

// NodeStatsはノード統計です
type NodeStats struct {
	NodeID            string
	RequestsHandled   int64
	SuccessfulReqs    int64
	ErrorReqs         int64
	TotalResponseTime time.Duration
	AvgResponseTime   time.Duration
	CurrentLoad       int64
	HealthStatus      NodeHealth
}

// StrategyStatsは戦略統計です
type StrategyStats struct {
	Strategy        LoadBalancingStrategy
	RequestsHandled int64
	SuccessRate     float64
	AvgResponseTime time.Duration
}

// HealthCheckerはヘルスチェッカーです
type HealthChecker struct {
	balancer          *AdaptiveLoadBalancer
	checkInterval     time.Duration
	timeoutDuration   time.Duration
	failureThreshold  int
	recoveryThreshold int
}

// RequestProcessorは要求プロセッサーです
type RequestProcessor struct {
	ID       int
	balancer *AdaptiveLoadBalancer
}

// LoadBalancerConfigは負荷分散設定です
type LoadBalancerConfig struct {
	FanOutFactor          int
	MaxConcurrentRequests int
	QueueSize             int
	Strategy              LoadBalancingStrategy
	HealthCheckInterval   time.Duration
	HealthCheckTimeout    time.Duration
	FailureThreshold      int
	RecoveryThreshold     int
	AdaptiveThreshold     float64
	EnableMetrics         bool
	MetricsInterval       time.Duration
}

// NewLoadBalancerConfigはデフォルト設定を作成します
func NewLoadBalancerConfig() *LoadBalancerConfig {
	return &LoadBalancerConfig{
		FanOutFactor:          3,
		MaxConcurrentRequests: 1000,
		QueueSize:             5000,
		Strategy:              StrategyAdaptive,
		HealthCheckInterval:   10 * time.Second,
		HealthCheckTimeout:    5 * time.Second,
		FailureThreshold:      3,
		RecoveryThreshold:     5,
		AdaptiveThreshold:     0.8, // 80%の負荷で戦略変更
		EnableMetrics:         true,
		MetricsInterval:       15 * time.Second,
	}
}

// NewAdaptiveLoadBalancerは新しい適応型負荷分散システムを作成します
func NewAdaptiveLoadBalancer(config *LoadBalancerConfig) *AdaptiveLoadBalancer {
	ctx, cancel := context.WithCancel(context.Background())

	balancer := &AdaptiveLoadBalancer{
		nodes:         make(map[string]*LoadBalancerNode),
		strategy:      config.Strategy,
		fanOutFactor:  config.FanOutFactor,
		maxConcurrent: config.MaxConcurrentRequests,
		requestQueue:  make(chan RequestTask, config.QueueSize),
		resultQueue:   make(chan ProcessingResult, config.QueueSize),
		ctx:           ctx,
		cancel:        cancel,
		stats: &LoadBalancerStats{
			nodeStats:     make(map[string]*NodeStats),
			strategyStats: make(map[LoadBalancingStrategy]*StrategyStats),
			startTime:     time.Now(),
		},
	}

	// ヘルスチェッカーを初期化
	balancer.healthChecker = &HealthChecker{
		balancer:          balancer,
		checkInterval:     config.HealthCheckInterval,
		timeoutDuration:   config.HealthCheckTimeout,
		failureThreshold:  config.FailureThreshold,
		recoveryThreshold: config.RecoveryThreshold,
	}

	return balancer
}

// AddNodeはノードを追加します
func (alb *AdaptiveLoadBalancer) AddNode(node *LoadBalancerNode) {
	alb.nodesMutex.Lock()
	defer alb.nodesMutex.Unlock()

	alb.nodes[node.ID] = node
	alb.stats.nodeStats[node.ID] = &NodeStats{
		NodeID:       node.ID,
		HealthStatus: HealthHealthy,
	}

	log.Printf("Node added: %s (%s) with weight %d and capacity %d",
		node.ID, node.Address, node.Weight, node.MaxCapacity)
}

// RemoveNodeはノードを削除します
func (alb *AdaptiveLoadBalancer) RemoveNode(nodeID string) {
	alb.nodesMutex.Lock()
	defer alb.nodesMutex.Unlock()

	delete(alb.nodes, nodeID)
	delete(alb.stats.nodeStats, nodeID)

	log.Printf("Node removed: %s", nodeID)
}

// Startは負荷分散システムを開始します
func (alb *AdaptiveLoadBalancer) Start() error {
	if !atomic.CompareAndSwapInt32(&alb.isRunning, 0, 1) {
		return fmt.Errorf("load balancer is already running")
	}

	alb.startTime = time.Now()
	alb.stats.startTime = alb.startTime

	log.Printf("Starting adaptive load balancer with %d nodes", len(alb.nodes))

	// 要求プロセッサーを開始
	for i := 0; i < 5; i++ {
		processor := &RequestProcessor{
			ID:       i,
			balancer: alb,
		}

		alb.wg.Add(1)
		go processor.run()
	}

	// 結果処理を開始
	alb.wg.Add(1)
	go alb.handleResults()

	// ヘルスチェックを開始
	alb.wg.Add(1)
	go alb.healthChecker.run()

	// 適応制御を開始
	alb.wg.Add(1)
	go alb.adaptiveControl()

	// メトリクス監視を開始
	alb.wg.Add(1)
	go alb.monitorMetrics()

	log.Printf("Adaptive load balancer started successfully")
	return nil
}

// runはプロセッサーのメインループです
func (rp *RequestProcessor) run() {
	defer rp.balancer.wg.Done()

	log.Printf("Request processor %d started", rp.ID)

	for {
		select {
		case <-rp.balancer.ctx.Done():
			log.Printf("Processor %d stopping due to context cancellation", rp.ID)
			return
		case request, ok := <-rp.balancer.requestQueue:
			if !ok {
				log.Printf("Processor %d stopping due to request queue closure", rp.ID)
				return
			}

			// 要求を処理
			rp.processRequest(request)
		}
	}
}

// processRequestは要求を処理します
func (rp *RequestProcessor) processRequest(request RequestTask) {
	start := time.Now()

	// Fan-out戦略に基づいてノードを選択
	selectedNodes := rp.balancer.selectNodes(request)

	if len(selectedNodes) == 0 {
		// 利用可能なノードがない場合
		result := ProcessingResult{
			TaskID:      request.ID,
			Success:     false,
			Error:       fmt.Errorf("no available nodes"),
			QueueTime:   time.Since(request.SubmittedAt),
			CompletedAt: time.Now(),
		}

		select {
		case rp.balancer.resultQueue <- result:
		case <-rp.balancer.ctx.Done():
			return
		}
		return
	}

	// Fan-outパターンで複数ノードに並行して要求を送信
	resultChan := make(chan ProcessingResult, len(selectedNodes))
	ctx, cancel := context.WithTimeout(rp.balancer.ctx, request.Timeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, node := range selectedNodes {
		wg.Add(1)
		go func(n *LoadBalancerNode) {
			defer wg.Done()
			result := rp.processOnNode(ctx, request, n, start)

			select {
			case resultChan <- result:
			case <-ctx.Done():
			}
		}(node)
	}

	// 全ての結果を待機
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 結果を収集（Fan-in）
	results := make([]ProcessingResult, 0, len(selectedNodes))
	for result := range resultChan {
		results = append(results, result)

		// 最初の成功結果を返す（早期終了）
		if result.Success {
			cancel() // 他の処理をキャンセル

			select {
			case rp.balancer.resultQueue <- result:
			case <-rp.balancer.ctx.Done():
				return
			}
			return
		}
	}

	// すべて失敗した場合は最初のエラーを返す
	if len(results) > 0 {
		select {
		case rp.balancer.resultQueue <- results[0]:
		case <-rp.balancer.ctx.Done():
			return
		}
	}
}

// processOnNodeはノードで要求を処理します
func (rp *RequestProcessor) processOnNode(ctx context.Context, request RequestTask, node *LoadBalancerNode, startTime time.Time) ProcessingResult {
	queueTime := time.Since(request.SubmittedAt)
	processStart := time.Now()

	// ノードの負荷を増加
	atomic.AddInt64(&node.CurrentLoad, 1)
	defer atomic.AddInt64(&node.CurrentLoad, -1)

	// 実際の処理をシミュレート
	result := rp.simulateNodeProcessing(ctx, request, node)

	nodeProcessTime := time.Since(processStart)
	totalProcessTime := time.Since(startTime) // 処理開始からの総時間を使用

	// ノード統計を更新
	rp.balancer.updateNodeStats(node.ID, result.Success, nodeProcessTime)

	result.TaskID = request.ID
	result.NodeID = node.ID
	result.ProcessTime = totalProcessTime
	result.QueueTime = queueTime
	result.CompletedAt = time.Now()

	return result
}

// simulateNodeProcessingはノード処理をシミュレートします
func (rp *RequestProcessor) simulateNodeProcessing(ctx context.Context, request RequestTask, node *LoadBalancerNode) ProcessingResult {
	// 処理時間をシミュレート（ノードの負荷に基づいて調整）
	baseProcessTime := time.Duration(rand.Intn(200)+50) * time.Millisecond
	loadFactor := float64(atomic.LoadInt64(&node.CurrentLoad)) / float64(node.MaxCapacity)
	adjustedProcessTime := time.Duration(float64(baseProcessTime) * (1 + loadFactor))

	// 段階的に処理をシミュレート（コンテキストキャンセレーションをチェック）
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	processStart := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ProcessingResult{
				Success: false,
				Error:   ctx.Err(),
			}
		case <-ticker.C:
			if time.Since(processStart) >= adjustedProcessTime {
				// 処理完了

				// ノードの健全性に基づいてランダムにエラーを発生
				errorRate := rp.calculateErrorRate(node)
				if rand.Float64() < errorRate {
					return ProcessingResult{
						Success: false,
						Error:   fmt.Errorf("processing error on node %s", node.ID),
					}
				}

				return ProcessingResult{
					Success: true,
					Result:  fmt.Sprintf("processed_request_%d_on_node_%s", request.ID, node.ID),
				}
			}
		}
	}
}

// calculateErrorRateはノードのエラー率を計算します
func (rp *RequestProcessor) calculateErrorRate(node *LoadBalancerNode) float64 {
	switch node.Health {
	case HealthHealthy:
		return 0.02 // 2%
	case HealthDegraded:
		return 0.10 // 10%
	case HealthUnhealthy:
		return 0.50 // 50%
	default:
		return 0.20 // 20%
	}
}

// selectNodesは要求に適したノードを選択します
func (alb *AdaptiveLoadBalancer) selectNodes(request RequestTask) []*LoadBalancerNode {
	alb.nodesMutex.RLock()
	defer alb.nodesMutex.RUnlock()

	// 健全なノードをフィルタリング
	var availableNodes []*LoadBalancerNode
	for _, node := range alb.nodes {
		if node.Health == HealthHealthy || node.Health == HealthDegraded {
			if atomic.LoadInt64(&node.CurrentLoad) < node.MaxCapacity {
				availableNodes = append(availableNodes, node)
			}
		}
	}

	if len(availableNodes) == 0 {
		return nil
	}

	// 戦略に基づいてノードを選択
	selectedNodes := alb.selectByStrategy(availableNodes, request)

	// Fan-out数を制限
	fanOutCount := alb.fanOutFactor
	if request.RequiredNodes > 0 && request.RequiredNodes < fanOutCount {
		fanOutCount = request.RequiredNodes
	}
	if len(selectedNodes) > fanOutCount {
		selectedNodes = selectedNodes[:fanOutCount]
	}

	atomic.AddInt64(&alb.stats.totalFanOutRequests, int64(len(selectedNodes)))

	return selectedNodes
}

// selectByStrategyは戦略に基づいてノードを選択します
func (alb *AdaptiveLoadBalancer) selectByStrategy(nodes []*LoadBalancerNode, request RequestTask) []*LoadBalancerNode {
	switch alb.strategy {
	case StrategyRoundRobin:
		return alb.selectRoundRobin(nodes)
	case StrategyWeightedRoundRobin:
		return alb.selectWeightedRoundRobin(nodes)
	case StrategyLeastConnections:
		return alb.selectLeastConnections(nodes)
	case StrategyLeastResponseTime:
		return alb.selectLeastResponseTime(nodes)
	case StrategyAdaptive:
		return alb.selectAdaptive(nodes, request)
	default:
		return alb.selectRoundRobin(nodes)
	}
}

// selectRoundRobinはラウンドロビン選択を実行します
func (alb *AdaptiveLoadBalancer) selectRoundRobin(nodes []*LoadBalancerNode) []*LoadBalancerNode {
	if len(nodes) == 0 {
		return nil
	}

	// 簡単なラウンドロビン実装
	index := int(atomic.AddInt64(&alb.stats.totalRequests, 1)) % len(nodes)
	return []*LoadBalancerNode{nodes[index]}
}

// selectWeightedRoundRobinは重み付きラウンドロビン選択を実行します
func (alb *AdaptiveLoadBalancer) selectWeightedRoundRobin(nodes []*LoadBalancerNode) []*LoadBalancerNode {
	if len(nodes) == 0 {
		return nil
	}

	// 重みの合計を計算
	totalWeight := 0
	for _, node := range nodes {
		totalWeight += node.Weight
	}

	if totalWeight == 0 {
		return alb.selectRoundRobin(nodes)
	}

	// ランダムに重み付き選択
	target := rand.Intn(totalWeight)
	cumulative := 0

	for _, node := range nodes {
		cumulative += node.Weight
		if cumulative > target {
			return []*LoadBalancerNode{node}
		}
	}

	return []*LoadBalancerNode{nodes[0]}
}

// selectLeastConnectionsは最少接続数選択を実行します
func (alb *AdaptiveLoadBalancer) selectLeastConnections(nodes []*LoadBalancerNode) []*LoadBalancerNode {
	if len(nodes) == 0 {
		return nil
	}

	// 負荷でソート
	sort.Slice(nodes, func(i, j int) bool {
		return atomic.LoadInt64(&nodes[i].CurrentLoad) < atomic.LoadInt64(&nodes[j].CurrentLoad)
	})

	return []*LoadBalancerNode{nodes[0]}
}

// selectLeastResponseTimeは最短応答時間選択を実行します
func (alb *AdaptiveLoadBalancer) selectLeastResponseTime(nodes []*LoadBalancerNode) []*LoadBalancerNode {
	if len(nodes) == 0 {
		return nil
	}

	// 応答時間でソート
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ResponseTime < nodes[j].ResponseTime
	})

	return []*LoadBalancerNode{nodes[0]}
}

// selectAdaptiveは適応的選択を実行します
func (alb *AdaptiveLoadBalancer) selectAdaptive(nodes []*LoadBalancerNode, request RequestTask) []*LoadBalancerNode {
	if len(nodes) == 0 {
		return nil
	}

	// 複合スコアに基づいて選択
	type nodeScore struct {
		node  *LoadBalancerNode
		score float64
	}

	var scores []nodeScore
	for _, node := range nodes {
		score := alb.calculateAdaptiveScore(node, request)
		scores = append(scores, nodeScore{node: node, score: score})
	}

	// スコアでソート（高い方が良い）
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	// 上位ノードを返す
	result := make([]*LoadBalancerNode, 0, alb.fanOutFactor)
	for i := 0; i < len(scores) && i < alb.fanOutFactor; i++ {
		result = append(result, scores[i].node)
	}

	return result
}

// calculateAdaptiveScoreは適応的スコアを計算します
func (alb *AdaptiveLoadBalancer) calculateAdaptiveScore(node *LoadBalancerNode, request RequestTask) float64 {
	// 負荷率（低い方が良い）
	loadRate := float64(atomic.LoadInt64(&node.CurrentLoad)) / float64(node.MaxCapacity)
	loadScore := 1.0 - loadRate

	// 応答時間（短い方が良い）
	responseScore := 1.0 / (1.0 + node.ResponseTime.Seconds())

	// 成功率
	successScore := node.SuccessRate

	// 重み
	weightScore := float64(node.Weight) / 10.0

	// 健全性
	var healthScore float64
	switch node.Health {
	case HealthHealthy:
		healthScore = 1.0
	case HealthDegraded:
		healthScore = 0.7
	case HealthUnhealthy:
		healthScore = 0.3
	default:
		healthScore = 0.5
	}

	// 優先度に基づく調整
	priorityMultiplier := 1.0 + float64(request.Priority)*0.1

	// 総合スコア計算
	totalScore := (loadScore*0.3 + responseScore*0.2 + successScore*0.2 + weightScore*0.1 + healthScore*0.2) * priorityMultiplier

	return totalScore
}

// updateNodeStatsはノード統計を更新します
func (alb *AdaptiveLoadBalancer) updateNodeStats(nodeID string, success bool, responseTime time.Duration) {
	alb.stats.mu.Lock()
	defer alb.stats.mu.Unlock()

	stats, exists := alb.stats.nodeStats[nodeID]
	if !exists {
		return
	}

	stats.RequestsHandled++
	stats.TotalResponseTime += responseTime
	stats.AvgResponseTime = stats.TotalResponseTime / time.Duration(stats.RequestsHandled)
	stats.CurrentLoad = atomic.LoadInt64(&alb.nodes[nodeID].CurrentLoad)

	if success {
		stats.SuccessfulReqs++
	} else {
		stats.ErrorReqs++
	}

	// ノードの成功率を更新
	if node, exists := alb.nodes[nodeID]; exists {
		node.SuccessRate = float64(stats.SuccessfulReqs) / float64(stats.RequestsHandled)
		node.ResponseTime = stats.AvgResponseTime
	}
}

// SubmitRequestは要求をキューに追加します
func (alb *AdaptiveLoadBalancer) SubmitRequest(request RequestTask) error {
	if atomic.LoadInt32(&alb.isRunning) == 0 {
		return fmt.Errorf("load balancer is not running")
	}

	// タイムスタンプを設定
	if request.SubmittedAt.IsZero() {
		request.SubmittedAt = time.Now()
	}

	// デフォルトタイムアウトを設定
	if request.Timeout == 0 {
		request.Timeout = 30 * time.Second
	}

	select {
	case alb.requestQueue <- request:
		atomic.AddInt64(&alb.stats.totalRequests, 1)
		return nil
	case <-alb.ctx.Done():
		return fmt.Errorf("load balancer is shutting down")
	default:
		return fmt.Errorf("request queue is full")
	}
}

// GetResultChannelは結果チャネルを取得します
func (alb *AdaptiveLoadBalancer) GetResultChannel() <-chan ProcessingResult {
	return alb.resultQueue
}

// handleResultsは結果を処理します
func (alb *AdaptiveLoadBalancer) handleResults() {
	defer alb.wg.Done()

	log.Println("Result handler started")

	for {
		select {
		case <-alb.ctx.Done():
			log.Println("Result handler stopping due to context cancellation")
			return
		case result, ok := <-alb.resultQueue:
			if !ok {
				log.Println("Result handler stopping due to result queue closure")
				return
			}

			// 統計更新
			if result.Success {
				atomic.AddInt64(&alb.stats.totalSuccessful, 1)
			} else {
				atomic.AddInt64(&alb.stats.totalErrors, 1)
			}

			// 平均応答時間を更新
			completed := atomic.LoadInt64(&alb.stats.totalSuccessful) + atomic.LoadInt64(&alb.stats.totalErrors)
			if completed > 0 {
				alb.stats.mu.Lock()
				alb.stats.averageResponseTime = time.Duration(
					(int64(alb.stats.averageResponseTime)*completed + int64(result.ProcessTime)) / (completed + 1))
				alb.stats.mu.Unlock()
			}

			// 結果をログ出力
			if result.Error != nil {
				log.Printf("Request %d failed on node %s: %v (QueueTime=%v, ProcessTime=%v)",
					result.TaskID, result.NodeID, result.Error, result.QueueTime, result.ProcessTime)
			} else {
				log.Printf("Request %d completed on node %s: %v (QueueTime=%v, ProcessTime=%v)",
					result.TaskID, result.NodeID, result.Result, result.QueueTime, result.ProcessTime)
			}
		}
	}
}

// runはヘルスチェッカーのメインループです
func (hc *HealthChecker) run() {
	defer hc.balancer.wg.Done()

	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	log.Println("Health checker started")

	for {
		select {
		case <-hc.balancer.ctx.Done():
			log.Println("Health checker stopping due to context cancellation")
			return
		case <-ticker.C:
			hc.performHealthChecks()
		}
	}
}

// performHealthChecksはヘルスチェックを実行します
func (hc *HealthChecker) performHealthChecks() {
	hc.balancer.nodesMutex.RLock()
	nodes := make([]*LoadBalancerNode, 0, len(hc.balancer.nodes))
	for _, node := range hc.balancer.nodes {
		nodes = append(nodes, node)
	}
	hc.balancer.nodesMutex.RUnlock()

	for _, node := range nodes {
		go hc.checkNodeHealth(node)
	}
}

// checkNodeHealthはノードのヘルスチェックを実行します
func (hc *HealthChecker) checkNodeHealth(node *LoadBalancerNode) {
	// ヘルスチェックをシミュレート
	ctx, cancel := context.WithTimeout(hc.balancer.ctx, hc.timeoutDuration)
	defer cancel()

	// 実際のヘルスチェック処理をシミュレート
	healthy := hc.simulateHealthCheck(ctx, node)

	// ヘルス状態を更新
	hc.updateNodeHealth(node, healthy)

	node.LastHealthCheck = time.Now()
}

// simulateHealthCheckはヘルスチェックをシミュレートします
func (hc *HealthChecker) simulateHealthCheck(ctx context.Context, node *LoadBalancerNode) bool {
	// ノードの負荷と現在の健全性に基づいてヘルスチェック結果を調整
	loadRate := float64(atomic.LoadInt64(&node.CurrentLoad)) / float64(node.MaxCapacity)

	// 負荷率に基づいて成功率を調整
	var successRate float64
	switch node.Health {
	case HealthHealthy:
		successRate = 0.95 - loadRate*0.1 // 95%から負荷に応じて減少
	case HealthDegraded:
		successRate = 0.80 - loadRate*0.2 // 80%から負荷に応じて減少
	case HealthUnhealthy:
		successRate = 0.30 - loadRate*0.1 // 30%から負荷に応じて減少
	default:
		successRate = 0.70
	}

	select {
	case <-time.After(time.Duration(rand.Intn(int(hc.timeoutDuration.Milliseconds()))) * time.Millisecond):
		return rand.Float64() < successRate
	case <-ctx.Done():
		return false // タイムアウト
	}
}

// updateNodeHealthはノードヘルス状態を更新します
func (hc *HealthChecker) updateNodeHealth(node *LoadBalancerNode, healthy bool) {
	hc.balancer.nodesMutex.Lock()
	defer hc.balancer.nodesMutex.Unlock()

	currentHealth := node.Health

	if healthy {
		// 健全性回復
		switch currentHealth {
		case HealthUnhealthy:
			node.Health = HealthDegraded
			log.Printf("Node %s health improved: UNHEALTHY -> DEGRADED", node.ID)
		case HealthDegraded:
			node.Health = HealthHealthy
			log.Printf("Node %s health recovered: DEGRADED -> HEALTHY", node.ID)
		}
	} else {
		// 健全性悪化
		switch currentHealth {
		case HealthHealthy:
			node.Health = HealthDegraded
			log.Printf("Node %s health degraded: HEALTHY -> DEGRADED", node.ID)
		case HealthDegraded:
			node.Health = HealthUnhealthy
			log.Printf("Node %s health failed: DEGRADED -> UNHEALTHY", node.ID)
		}
	}

	// 統計更新
	if stats, exists := hc.balancer.stats.nodeStats[node.ID]; exists {
		stats.HealthStatus = node.Health
	}
}

// adaptiveControlは適応制御を実行します
func (alb *AdaptiveLoadBalancer) adaptiveControl() {
	defer alb.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Println("Adaptive control started")

	for {
		select {
		case <-alb.ctx.Done():
			log.Println("Adaptive control stopping due to context cancellation")
			return
		case <-ticker.C:
			alb.evaluateAndAdaptStrategy()
		}
	}
}

// evaluateAndAdaptStrategyは戦略を評価・適応します
func (alb *AdaptiveLoadBalancer) evaluateAndAdaptStrategy() {
	alb.stats.mu.RLock()
	totalRequests := atomic.LoadInt64(&alb.stats.totalRequests)
	totalSuccessful := atomic.LoadInt64(&alb.stats.totalSuccessful)
	totalErrors := atomic.LoadInt64(&alb.stats.totalErrors)
	alb.stats.mu.RUnlock()

	if totalRequests < 100 {
		return // 十分なデータがない
	}

	successRate := float64(totalSuccessful) / float64(totalRequests)
	errorRate := float64(totalErrors) / float64(totalRequests)

	// 現在の戦略の性能を評価
	log.Printf("Current strategy performance: Success=%.2f%%, Error=%.2f%%, Strategy=%v",
		successRate*100, errorRate*100, alb.strategy)

	// 性能が悪い場合は戦略を変更
	if errorRate > 0.2 { // 20%以上のエラー率
		newStrategy := alb.selectBetterStrategy()
		if newStrategy != alb.strategy {
			oldStrategy := alb.strategy
			alb.strategy = newStrategy
			log.Printf("Strategy adapted: %v -> %v (due to high error rate: %.2f%%)",
				oldStrategy, newStrategy, errorRate*100)
		}
	}

	// システム負荷に基づく適応
	alb.adaptToSystemLoad()
}

// selectBetterStrategyはより良い戦略を選択します
func (alb *AdaptiveLoadBalancer) selectBetterStrategy() LoadBalancingStrategy {
	// 現在の状況に基づいて最適な戦略を選択

	// ノードの健全性をチェック
	healthyNodes := 0
	degradedNodes := 0
	unhealthyNodes := 0

	alb.nodesMutex.RLock()
	for _, node := range alb.nodes {
		switch node.Health {
		case HealthHealthy:
			healthyNodes++
		case HealthDegraded:
			degradedNodes++
		case HealthUnhealthy:
			unhealthyNodes++
		}
	}
	alb.nodesMutex.RUnlock()

	totalNodes := healthyNodes + degradedNodes + unhealthyNodes

	// 戦略選択ロジック
	if float64(unhealthyNodes)/float64(totalNodes) > 0.3 {
		// 多くのノードが不健全な場合は最少接続戦略
		return StrategyLeastConnections
	} else if degradedNodes > healthyNodes {
		// 劣化ノードが多い場合は応答時間基準
		return StrategyLeastResponseTime
	} else {
		// 通常時は適応戦略
		return StrategyAdaptive
	}
}

// adaptToSystemLoadはシステム負荷に適応します
func (alb *AdaptiveLoadBalancer) adaptToSystemLoad() {
	// 現在のシステム負荷を計算
	totalCapacity := int64(0)
	totalCurrentLoad := int64(0)

	alb.nodesMutex.RLock()
	for _, node := range alb.nodes {
		totalCapacity += node.MaxCapacity
		totalCurrentLoad += atomic.LoadInt64(&node.CurrentLoad)
	}
	alb.nodesMutex.RUnlock()

	if totalCapacity == 0 {
		return
	}

	loadRate := float64(totalCurrentLoad) / float64(totalCapacity)

	// 負荷に基づいてFan-out係数を調整
	if loadRate > 0.8 {
		// 高負荷時はFan-out数を減らす
		if alb.fanOutFactor > 1 {
			alb.fanOutFactor--
			log.Printf("Reduced fan-out factor to %d due to high load (%.1f%%)", alb.fanOutFactor, loadRate*100)
		}
	} else if loadRate < 0.3 {
		// 低負荷時はFan-out数を増やす
		if alb.fanOutFactor < 5 {
			alb.fanOutFactor++
			log.Printf("Increased fan-out factor to %d due to low load (%.1f%%)", alb.fanOutFactor, loadRate*100)
		}
	}
}

// monitorMetricsはメトリクスを監視します
func (alb *AdaptiveLoadBalancer) monitorMetrics() {
	defer alb.wg.Done()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-alb.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			alb.reportMetrics()
		}
	}
}

// reportMetricsはメトリクスを報告します
func (alb *AdaptiveLoadBalancer) reportMetrics() {
	alb.stats.mu.RLock()
	defer alb.stats.mu.RUnlock()

	totalRequests := atomic.LoadInt64(&alb.stats.totalRequests)
	totalSuccessful := atomic.LoadInt64(&alb.stats.totalSuccessful)
	totalErrors := atomic.LoadInt64(&alb.stats.totalErrors)
	totalFanOut := atomic.LoadInt64(&alb.stats.totalFanOutRequests)

	uptime := time.Since(alb.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(totalRequests) / uptime.Seconds()
	}

	var successRate float64
	if totalRequests > 0 {
		successRate = float64(totalSuccessful) / float64(totalRequests) * 100
	}

	log.Printf("Load Balancer Metrics: Requests=%d, Success=%.1f%%, Errors=%d, FanOut=%d",
		totalRequests, successRate, totalErrors, totalFanOut)
	log.Printf("Performance: Throughput=%.2f req/sec, AvgResponseTime=%v, Strategy=%v, FanOutFactor=%d",
		throughput, alb.stats.averageResponseTime, alb.strategy, alb.fanOutFactor)

	// ノード別統計
	alb.nodesMutex.RLock()
	for nodeID, stats := range alb.stats.nodeStats {
		if node, exists := alb.nodes[nodeID]; exists {
			log.Printf("Node %s: Requests=%d, Success=%d, Errors=%d, Load=%d/%d, Health=%v, AvgTime=%v",
				nodeID, stats.RequestsHandled, stats.SuccessfulReqs, stats.ErrorReqs,
				stats.CurrentLoad, node.MaxCapacity, node.Health, stats.AvgResponseTime)
		}
	}
	alb.nodesMutex.RUnlock()
}

// Shutdownは負荷分散システムを停止します
func (alb *AdaptiveLoadBalancer) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&alb.isRunning, 1, 0) {
		return fmt.Errorf("load balancer is not running")
	}

	log.Println("Shutting down adaptive load balancer...")

	// 1. 新しい要求の受付を停止
	close(alb.requestQueue)

	// 2. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		alb.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown...")
		alb.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	// 3. チャネルを閉じる
	close(alb.resultQueue)

	log.Println("Adaptive load balancer shutdown completed")
	return nil
}

// GetStatsは統計情報を取得します
func (alb *AdaptiveLoadBalancer) GetStats() map[string]interface{} {
	alb.stats.mu.RLock()
	defer alb.stats.mu.RUnlock()

	totalRequests := atomic.LoadInt64(&alb.stats.totalRequests)
	totalSuccessful := atomic.LoadInt64(&alb.stats.totalSuccessful)
	totalErrors := atomic.LoadInt64(&alb.stats.totalErrors)
	totalFanOut := atomic.LoadInt64(&alb.stats.totalFanOutRequests)

	uptime := time.Since(alb.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(totalRequests) / uptime.Seconds()
	}

	nodeStats := make(map[string]interface{})
	for nodeID, stats := range alb.stats.nodeStats {
		nodeStats[nodeID] = map[string]interface{}{
			"requests_handled":      stats.RequestsHandled,
			"successful_requests":   stats.SuccessfulReqs,
			"error_requests":        stats.ErrorReqs,
			"current_load":          stats.CurrentLoad,
			"average_response_time": stats.AvgResponseTime,
			"health_status":         stats.HealthStatus,
		}
	}

	return map[string]interface{}{
		"total_requests":         totalRequests,
		"total_successful":       totalSuccessful,
		"total_errors":           totalErrors,
		"total_fan_out_requests": totalFanOut,
		"average_response_time":  alb.stats.averageResponseTime,
		"throughput_rps":         throughput,
		"uptime":                 uptime,
		"current_strategy":       alb.strategy,
		"fan_out_factor":         alb.fanOutFactor,
		"node_count":             len(alb.nodes),
		"node_stats":             nodeStats,
	}
}

func main() {
	// 負荷分散設定
	config := NewLoadBalancerConfig()
	config.FanOutFactor = 2
	config.MaxConcurrentRequests = 500
	config.Strategy = StrategyAdaptive

	// 負荷分散システム作成
	balancer := NewAdaptiveLoadBalancer(config)

	// テスト用ノードを追加
	nodes := []*LoadBalancerNode{
		{ID: "node1", Address: "192.168.1.10:8080", Weight: 10, MaxCapacity: 50, Health: HealthHealthy},
		{ID: "node2", Address: "192.168.1.11:8080", Weight: 8, MaxCapacity: 40, Health: HealthHealthy},
		{ID: "node3", Address: "192.168.1.12:8080", Weight: 12, MaxCapacity: 60, Health: HealthHealthy},
		{ID: "node4", Address: "192.168.1.13:8080", Weight: 6, MaxCapacity: 30, Health: HealthDegraded},
	}

	for _, node := range nodes {
		balancer.AddNode(node)
	}

	// システム開始
	if err := balancer.Start(); err != nil {
		log.Fatalf("Failed to start load balancer: %v", err)
	}

	// 結果処理のgoroutineを開始
	go func() {
		for result := range balancer.GetResultChannel() {
			if result.Success {
				log.Printf("✓ Request %d completed on %s: %v", result.TaskID, result.NodeID, result.Result)
			} else {
				log.Printf("✗ Request %d failed on %s: %v", result.TaskID, result.NodeID, result.Error)
			}
		}
	}()

	// テスト要求を送信
	go func() {
		requestTypes := []string{"query", "update", "delete", "create", "search"}

		for i := 0; i < 2000; i++ {
			request := RequestTask{
				ID:            int64(i),
				RequestType:   requestTypes[rand.Intn(len(requestTypes))],
				Payload:       []byte(fmt.Sprintf("request_data_%d", i)),
				Priority:      rand.Intn(5),
				Timeout:       time.Duration(rand.Intn(10)+5) * time.Second,
				RequiredNodes: rand.Intn(3) + 1,
				Metadata: map[string]interface{}{
					"client_id": fmt.Sprintf("client_%d", rand.Intn(10)),
					"region":    []string{"us-east", "us-west", "eu-west", "ap-south"}[rand.Intn(4)],
				},
			}

			if err := balancer.SubmitRequest(request); err != nil {
				log.Printf("Failed to submit request %d: %v", i, err)
				break
			}

			// 送信頻度を制御
			time.Sleep(time.Duration(rand.Intn(100)+20) * time.Millisecond)
		}

		log.Println("All test requests submitted")
	}()

	// 50秒間実行
	time.Sleep(50 * time.Second)

	// 最終統計表示
	stats := balancer.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := balancer.Shutdown(15 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
