package danmaku

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testDictionary(terms ...string) string {
	result := "category\tterm\tsource\n"
	for _, term := range terms {
		result += "ABUSE\t" + term + "\t测试样例\n"
	}
	return result
}

func TestModeratorSuffixOverlapAndUnicode(t *testing.T) {
	moderator, err := ReadModerator(strings.NewReader(testDictionary("he", "she", "hers", "his", "违规词", "fuck you")))
	if err != nil {
		t.Fatal(err)
	}
	matches := moderator.Check("ushers")
	var ranges []string
	for _, match := range matches {
		ranges = append(ranges, fmt.Sprintf("%d:%d", match.Start, match.End))
	}
	sort.Strings(ranges)
	if strings.Join(ranges, ",") != "1:4,2:4,2:6" {
		t.Fatalf("后缀或重叠匹配错误: %v", ranges)
	}
	if len(moderator.Check("ＦＵＣＫ ＹＯＵ")) != 1 || len(moderator.Check("违\u200b规词")) != 1 {
		t.Fatal("规范化副本未命中")
	}
	for _, normal := range []string{"大家晚上好😀", "学习防诈骗知识", "注意远离赌博", "good evening"} {
		if len(moderator.Check(normal)) != 0 {
			t.Fatalf("正常测试文本被拦: %q", normal)
		}
	}
	// 同一个只读自动机由多个连接并发使用，不共享扫描游标。
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				if len(moderator.Check("违规词")) != 1 {
					t.Error("并发扫描结果变化")
				}
			}
		}()
	}
	workers.Wait()
}

func TestModeratorConfigurationAndVersion(t *testing.T) {
	for _, raw := range []string{"", "ABUSE\t单\t来源\n", "ABUSE\t违规词\t\n", testDictionary("违规词", "违规词"), "UNKNOWN\t违规词\t来源\n", testDictionary("AB", "ａｂ")} {
		if _, err := ReadModerator(strings.NewReader(raw)); err == nil {
			t.Fatalf("坏词库被接受: %q", raw)
		}
	}
	first, _ := ReadModerator(strings.NewReader(testDictionary("违规词", "拒绝词")))
	second, _ := ReadModerator(strings.NewReader(testDictionary("拒绝词", "违规词")))
	if first.Version != second.Version {
		t.Fatal("词库排序不应改变审核语义版本")
	}
	production, err := LoadModerator("../../configs/moderation_words.tsv")
	if err != nil {
		t.Fatal(err)
	}
	if len(production.words) < 80 || len(production.words) > 100 {
		t.Fatal("初始词库数量不符")
	}
	if len(production.Check("刷单垫付")) == 0 || len(production.Check("大家好，欢迎来到直播间！")) != 0 {
		t.Fatal("种子词库样例检查失败")
	}
}

func TestEventIdentityAndDecode(t *testing.T) {
	id := uuid.Must(uuid.NewV7()).String()
	original := "大家晚上好😀"
	event, err := NewRequest("1", "2", "3", id, original, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	event.Payload.Username = "alice"
	event.ModerationVersion = strings.Repeat("a", 64)
	raw, err := event.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeRecord(raw, "topic-id")
	if err != nil || record.Event.Payload.Content != original {
		t.Fatalf("冻结事件未能原样读取: %v", err)
	}
	again, _ := NewRequest("1", "2", "3", id, original, time.Now())
	changed, _ := NewRequest("1", "2", "3", id, "修改内容", time.Now())
	if again.EventID != event.EventID || again.RequestHash != event.RequestHash || changed.RequestHash == event.RequestHash {
		t.Fatal("身份或原文指纹错误")
	}
	raw.Key = []byte("2")
	if _, err := DecodeRecord(raw, "topic-id"); err == nil {
		t.Fatal("错误分区键被放行")
	}
	for _, text := range []string{"", "  \n", "\x00", "hello\x01", strings.Repeat("中", 201), string([]byte{0xff})} {
		if ValidateText(text) == nil {
			t.Fatalf("非法文本被接受: %q", text)
		}
	}
	if ValidateText(strings.Repeat("😀", 200)) != nil {
		t.Fatal("长度误按 UTF-8 字节计算")
	}
}
