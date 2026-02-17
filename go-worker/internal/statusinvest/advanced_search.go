package statusinvest

import (
	"context"
	"fmt"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
)

type AdvancedSearchService struct {
	client *httpclient.Client
}

func NewAdvancedSearchService(client *httpclient.Client) *AdvancedSearchService {
	return &AdvancedSearchService{client: client}
}

type Quote struct {
	Ticker string
	Price  float64
}

type advancedSearchResponse struct {
	List []advancedSearchItem `json:"list"`
}

type advancedSearchItem struct {
	Ticker string  `json:"ticker"`
	Price  float64 `json:"price"`
}

const advancedSearchURL = httpclient.StatusInvestBase + "/category/advancedsearchresultpaginated?search=%7B%22Gestao%22%3A%22%22%2C%22my_range%22%3A%220%3B20%22%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22percentualcaixa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22numerocotistas%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividend_cagr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22cota_cagr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22patrimonio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valorpatrimonialcota%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22numerocotas%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lastdividend%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D%7D&orderColumn=&isAsc=&page=0&take=600&CategoryType=2"

func (s *AdvancedSearchService) ListQuotes(ctx context.Context) ([]Quote, error) {
	var res advancedSearchResponse
	if err := s.client.GetJSONStatusInvest(ctx, advancedSearchURL, &res); err != nil {
		return nil, fmt.Errorf("statusinvest advanced search: %w", err)
	}

	out := make([]Quote, 0, len(res.List))
	for _, it := range res.List {
		if it.Ticker == "" || it.Price <= 0 {
			continue
		}
		out = append(out, Quote{Ticker: it.Ticker, Price: it.Price})
	}

	return out, nil
}
