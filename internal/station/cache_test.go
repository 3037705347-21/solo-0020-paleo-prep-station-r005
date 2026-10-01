package station

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"example.com/paleo-prep-station/internal/contracts"
	"example.com/paleo-prep-station/internal/domain"
	"example.com/paleo-prep-station/internal/repository"
)

var catalogSeq atomic.Uint64

func newTestService() *Service {
	return New(repository.NewMemory())
}

func setupApprovedPlan(t *testing.T, svc *Service) (specimenID, planID string) {
	t.Helper()
	ctx := context.Background()
	catalog := fmt.Sprintf("PPS-TEST-%d", catalogSeq.Add(1))
	specimen, err := svc.RegisterSpecimen(ctx, contracts.RegisterSpecimenRequest{
		CatalogNumber: catalog,
		FieldLabel:    "北坡-07",
		MatrixKind:    "粉砂岩",
	})
	if err != nil {
		t.Fatalf("register specimen: %v", err)
	}
	plan, err := svc.CreatePlan(ctx, specimen.ID, contracts.CreatePlanRequest{
		Objective:      "暴露主要关节面并做可逆加固",
		Method:         "手工显微剥离",
		Adhesive:       "可逆树脂",
		TargetExposure: 80,
	})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if _, err := svc.ApprovePlan(ctx, plan.ID, contracts.ApprovalRequest{ApprovedBy: "curator-lin"}); err != nil {
		t.Fatalf("approve plan: %v", err)
	}
	return specimen.ID, plan.ID
}

func logPercent(t *testing.T, svc *Service, planID string, percent int) {
	t.Helper()
	_, err := svc.LogProgress(context.Background(), planID, contracts.ProgressRequest{
		Stage:      "矩阵剥离",
		Percent:    percent,
		Note:       "缓存一致性检查",
		RecordedBy: "preparator-chen",
	})
	if err != nil {
		t.Fatalf("log progress %d%%: %v", percent, err)
	}
}

// 写入成功后，所有读取入口必须立即反映这次提交，而不是上一段比例或旧状态。
func TestWritesImmediatelyVisibleAtAllReadEntries(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()
	specimenID, planID := setupApprovedPlan(t, svc)

	// 先把三个读取入口的缓存全部预热。
	if _, err := svc.GetPlan(ctx, planID); err != nil {
		t.Fatalf("warm plan snapshot: %v", err)
	}
	if _, err := svc.ListPlansForSpecimen(ctx, specimenID); err != nil {
		t.Fatalf("warm plan list: %v", err)
	}
	if _, err := svc.GetSpecimen(ctx, specimenID); err != nil {
		t.Fatalf("warm specimen snapshot: %v", err)
	}

	logPercent(t, svc, planID, 35)

	snapshot, err := svc.GetPlan(ctx, planID)
	if err != nil {
		t.Fatalf("get plan after progress: %v", err)
	}
	if len(snapshot.Progress) != 1 || snapshot.Progress[0].Percent != 35 {
		t.Fatalf("plan detail still shows previous progress: %+v", snapshot.Progress)
	}
	if snapshot.Plan.Status != domain.PlanActive {
		t.Fatalf("plan detail status = %s, want active", snapshot.Plan.Status)
	}

	list, err := svc.ListPlansForSpecimen(ctx, specimenID)
	if err != nil {
		t.Fatalf("list plans after progress: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("plan list length = %d, want 1", len(list))
	}
	// 列表与详情必须来自同一次提交，不能一个是旧状态、一个是新状态。
	if list[0].Status != snapshot.Plan.Status || list[0].Version != snapshot.Plan.Version {
		t.Fatalf("plan list status %s/v%d differs from detail %s/v%d",
			list[0].Status, list[0].Version, snapshot.Plan.Status, snapshot.Plan.Version)
	}

	specimen, err := svc.GetSpecimen(ctx, specimenID)
	if err != nil {
		t.Fatalf("get specimen after progress: %v", err)
	}
	if specimen.Status != domain.SpecimenPreparing {
		t.Fatalf("specimen status = %s, want preparing", specimen.Status)
	}

	logPercent(t, svc, planID, 100)

	snapshot, err = svc.GetPlan(ctx, planID)
	if err != nil {
		t.Fatalf("get plan after completion: %v", err)
	}
	if snapshot.Plan.Status != domain.PlanCompleted || len(snapshot.Progress) != 2 {
		t.Fatalf("plan detail = %s with %d entries, want completed with 2",
			snapshot.Plan.Status, len(snapshot.Progress))
	}
	list, err = svc.ListPlansForSpecimen(ctx, specimenID)
	if err != nil {
		t.Fatalf("list plans after completion: %v", err)
	}
	if list[0].Status != domain.PlanCompleted {
		t.Fatalf("plan list status = %s, want completed", list[0].Status)
	}
	specimen, err = svc.GetSpecimen(ctx, specimenID)
	if err != nil {
		t.Fatalf("get specimen after completion: %v", err)
	}
	if specimen.Status != domain.SpecimenStabilized {
		t.Fatalf("specimen status = %s, want stabilized", specimen.Status)
	}

	if _, err := svc.ReleaseSpecimen(ctx, specimenID, contracts.ReleaseRequest{
		Purpose:    "比较解剖研究",
		Recipient:  "地层研究组",
		ApprovedBy: "keeper-zhou",
	}); err != nil {
		t.Fatalf("release specimen: %v", err)
	}
	specimen, err = svc.GetSpecimen(ctx, specimenID)
	if err != nil {
		t.Fatalf("get specimen after release: %v", err)
	}
	if specimen.Status != domain.SpecimenAvailable {
		t.Fatalf("specimen status = %s, want available", specimen.Status)
	}
}

// 高频读取与持续写入并发时不能读到数据竞争产生的脏数据；
// 写入结束后所有读取入口必须立刻收敛到最终提交状态。
func TestConcurrentReadsConvergeAfterWrite(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()
	specimenID, planID := setupApprovedPlan(t, svc)

	const writes = 40
	const readers = 8

	var readersWG sync.WaitGroup
	stop := make(chan struct{})
	var readerFailures atomic.Int32

	for i := 0; i < readers; i++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snapshot, err := svc.GetPlan(ctx, planID)
				if err != nil {
					readerFailures.Add(1)
					return
				}
				// 快照内部必须自洽：进度条数与序列号、方案状态不能跨提交混搭。
				if len(snapshot.Progress) > 0 {
					if snapshot.Plan.Status != domain.PlanActive && snapshot.Plan.Status != domain.PlanCompleted {
						readerFailures.Add(1)
						return
					}
					last := snapshot.Progress[len(snapshot.Progress)-1]
					if last.Sequence != len(snapshot.Progress) {
						readerFailures.Add(1)
						return
					}
				}
				if _, err := svc.ListPlansForSpecimen(ctx, specimenID); err != nil {
					readerFailures.Add(1)
					return
				}
				if _, err := svc.GetSpecimen(ctx, specimenID); err != nil {
					readerFailures.Add(1)
					return
				}
			}
		}()
	}

	for i := 0; i < writes; i++ {
		// 10, 12, ..., 88：保持单调且不提前完成方案。
		logPercent(t, svc, planID, 10+i*2)
	}
	logPercent(t, svc, planID, 100)
	close(stop)
	readersWG.Wait()

	if failures := readerFailures.Load(); failures > 0 {
		t.Fatalf("concurrent readers observed %d inconsistent views", failures)
	}

	snapshot, err := svc.GetPlan(ctx, planID)
	if err != nil {
		t.Fatalf("final plan detail: %v", err)
	}
	if snapshot.Plan.Status != domain.PlanCompleted {
		t.Fatalf("final plan status = %s, want completed", snapshot.Plan.Status)
	}
	if len(snapshot.Progress) != writes+1 {
		t.Fatalf("final progress count = %d, want %d", len(snapshot.Progress), writes+1)
	}
	if snapshot.Progress[len(snapshot.Progress)-1].Percent != 100 {
		t.Fatalf("final progress percent = %d, want 100",
			snapshot.Progress[len(snapshot.Progress)-1].Percent)
	}
	list, err := svc.ListPlansForSpecimen(ctx, specimenID)
	if err != nil {
		t.Fatalf("final plan list: %v", err)
	}
	if list[0].Status != domain.PlanCompleted || list[0].Version != snapshot.Plan.Version {
		t.Fatalf("final list status %s/v%d differs from detail %s/v%d",
			list[0].Status, list[0].Version, snapshot.Plan.Status, snapshot.Plan.Version)
	}
	specimen, err := svc.GetSpecimen(ctx, specimenID)
	if err != nil {
		t.Fatalf("final specimen: %v", err)
	}
	if specimen.Status != domain.SpecimenStabilized {
		t.Fatalf("final specimen status = %s, want stabilized", specimen.Status)
	}
}
