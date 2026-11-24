package danmaku

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

type word struct{ Term, Category string }
type node struct {
	Next    map[rune]int
	Failure int
	Output  []int
}

// Moderator 在启动时完整构建，之后不修改，多个连接可并发扫描。
type Moderator struct {
	nodes   []node
	words   []word
	Version string
}
type Match struct {
	Category   string
	Start, End int
}

// normalizeReview 只处理审核副本；不删除普通空格、标点或表情连接符。
func normalizeReview(value string) string {
	value = norm.NFKC.String(value)
	value = strings.ToLower(value)
	return strings.NewReplacer("\u200b", "", "\ufeff", "").Replace(value)
}

func LoadModerator(path string) (*Moderator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开审核词库: %w", err)
	}
	defer file.Close()
	return ReadModerator(file)
}

func ReadModerator(reader io.Reader) (*Moderator, error) {
	scanner := bufio.NewScanner(io.LimitReader(reader, 1024*1024+1))
	scanner.Buffer(make([]byte, 4096), 16*1024)
	moderator := &Moderator{nodes: []node{{Next: make(map[rune]int)}}}
	allowed := map[string]bool{"ABUSE": true, "THREAT": true, "SEXUAL": true, "GAMBLING": true, "FRAUD": true, "ILLEGAL": true}
	seen := make(map[string]bool)
	line, total := 0, 0
	for scanner.Scan() {
		line++
		raw := scanner.Text()
		total += len(raw) + 1
		if total > 1024*1024 {
			return nil, errors.New("词库超过 1 MiB 保护上限")
		}
		if line == 1 && raw == "category\tterm\tsource" {
			continue
		}
		if !utf8.ValidString(raw) {
			return nil, fmt.Errorf("词库第 %d 行不是 UTF-8", line)
		}
		fields := strings.Split(raw, "\t")
		if len(fields) != 3 || !allowed[fields[0]] || strings.TrimSpace(fields[2]) == "" {
			return nil, fmt.Errorf("词库第 %d 行格式无效", line)
		}
		term := normalizeReview(fields[1])
		size := utf8.RuneCountInString(term)
		if term != strings.TrimSpace(term) || size < 2 || size > 100 || seen[term] || len(moderator.words) >= 10000 {
			return nil, fmt.Errorf("词库第 %d 行为空、重复或超出保护上限", line)
		}
		seen[term] = true
		moderator.words = append(moderator.words, word{term, fields[0]})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(moderator.words) == 0 {
		return nil, errors.New("审核词库不能为空")
	}
	sort.Slice(moderator.words, func(i, j int) bool { return moderator.words[i].Term < moderator.words[j].Term })
	canonical := "nfkc-lower-remove-200b-feff/v1\n"
	for index, item := range moderator.words {
		canonical += item.Category + "\t" + item.Term + "\n"
		state := 0
		for _, ch := range item.Term {
			next, exists := moderator.nodes[state].Next[ch]
			if !exists {
				next = len(moderator.nodes)
				moderator.nodes = append(moderator.nodes, node{Next: make(map[rune]int)})
				moderator.nodes[state].Next[ch] = next
			}
			state = next
		}
		moderator.nodes[state].Output = append(moderator.nodes[state].Output, index)
	}
	sum := sha256.Sum256([]byte(canonical))
	moderator.Version = hex.EncodeToString(sum[:])
	// 广度优先构建失败指针，继承后缀输出，保证重叠词和后缀词不漏报。
	queue := make([]int, 0, len(moderator.nodes))
	for _, next := range moderator.nodes[0].Next {
		queue = append(queue, next)
	}
	for head := 0; head < len(queue); head++ {
		state := queue[head]
		for ch, next := range moderator.nodes[state].Next {
			fallback := moderator.nodes[state].Failure
			for fallback != 0 {
				if _, ok := moderator.nodes[fallback].Next[ch]; ok {
					break
				}
				fallback = moderator.nodes[fallback].Failure
			}
			if target, ok := moderator.nodes[fallback].Next[ch]; ok {
				moderator.nodes[next].Failure = target
			}
			inherited := moderator.nodes[moderator.nodes[next].Failure].Output
			moderator.nodes[next].Output = append(moderator.nodes[next].Output, inherited...)
			queue = append(queue, next)
		}
	}
	return moderator, nil
}

// Check 的索引以规范化副本的 rune 为单位，End 为不包含的右边界。
func (moderator *Moderator) Check(content string) []Match {
	text := []rune(normalizeReview(content))
	state := 0
	var matches []Match
	for index, ch := range text {
		for state != 0 {
			if _, ok := moderator.nodes[state].Next[ch]; ok {
				break
			}
			state = moderator.nodes[state].Failure
		}
		if next, ok := moderator.nodes[state].Next[ch]; ok {
			state = next
		}
		for _, output := range moderator.nodes[state].Output {
			item := moderator.words[output]
			matches = append(matches, Match{Category: item.Category, Start: index + 1 - utf8.RuneCountInString(item.Term), End: index + 1})
			if len(matches) >= 200 {
				return matches
			}
		}
	}
	return matches
}
