package model

type Product struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Category string `json:"category"`
	Price    int64  `json:"price"`
	Stock    int64  `json:"stock"`
}

type Order struct {
	OrderID   string `json:"orderId"`
	UserID    string `json:"userId"`
	ProductID string `json:"productId"`
}

type ProductList struct {
	Items []Product `json:"items"`
	Page  int64     `json:"page"`
	Limit int64     `json:"limit"`
	Total int64     `json:"total"`
}

type UserOrder struct {
	OrderID   string `json:"orderId"`
	ProductID string `json:"productId"`
}

type UserOrders struct {
	UserID string      `json:"userId"`
	Orders []UserOrder `json:"orders"`
}
