/// 账户管理模块：维护客户资金和持仓
///
/// 功能：
/// - 管理客户账户余额
/// - 追踪客户持仓
/// - 订单前资金/持仓验证
/// - 成交时更新资金/持仓
/// - 冻结/解冻资金和持仓
use std::collections::HashMap;

/// 账户ID类型
pub type AccountId = u64;

/// 客户持仓
#[derive(Clone, Copy, Debug)]
pub struct Position {
    /// 持仓数量
    pub quantity: f64,
    /// 冻结数量（待成交）
    pub frozen: f64,
}

impl Position {
    /// 可用持仓 = 持仓数量 - 冻结数量
    pub fn available(&self) -> f64 {
        (self.quantity - self.frozen).max(0.0)
    }

    /// 冻结部分持仓
    pub fn freeze(&mut self, qty: f64) -> bool {
        if self.available() >= qty {
            self.frozen += qty;
            true
        } else {
            false
        }
    }

    /// 释放冻结的持仓
    pub fn unfreeze(&mut self, qty: f64) {
        self.frozen = (self.frozen - qty).max(0.0);
    }

    /// 成交时扣减持仓
    pub fn execute_sell(&mut self, qty: f64) -> bool {
        if self.frozen >= qty {
            self.quantity -= qty;
            self.frozen -= qty;
            true
        } else {
            false
        }
    }

    /// 成交时增加持仓
    pub fn execute_buy(&mut self, qty: f64) {
        self.quantity += qty;
    }
}

/// 客户账户信息
#[derive(Clone, Debug)]
pub struct Account {
    /// 账户ID
    pub id: AccountId,
    /// 可用余额（CNY）
    pub balance: f64,
    /// 冻结余额（待成交订单占用）
    pub frozen_balance: f64,
    /// 持仓 (symbol -> position)
    pub positions: HashMap<String, Position>,
}

impl Account {
    /// 创建新账户
    pub fn new(id: AccountId, initial_balance: f64) -> Self {
        Self {
            id,
            balance: initial_balance,
            frozen_balance: 0.0,
            positions: HashMap::new(),
        }
    }

    /// 可用余额 = 余额 - 冻结余额
    pub fn available_balance(&self) -> f64 {
        (self.balance - self.frozen_balance).max(0.0)
    }

    /// 总资产 = 可用余额 + 所有持仓市值
    pub fn total_assets(&self, market_prices: &HashMap<String, f64>) -> f64 {
        let mut total = self.available_balance();
        for (symbol, position) in &self.positions {
            if let Some(price) = market_prices.get(symbol) {
                total += position.quantity * price;
            }
        }
        total
    }

    /// 获取持仓（如果不存在则创建空持仓）
    pub fn get_position_mut(&mut self, symbol: &str) -> &mut Position {
        self.positions
            .entry(symbol.to_string())
            .or_insert(Position {
                quantity: 0.0,
                frozen: 0.0,
            })
    }

    /// 获取持仓（只读）
    pub fn get_position(&self, symbol: &str) -> Option<Position> {
        self.positions.get(symbol).copied()
    }
}

/// 账户管理器
pub struct AccountManager {
    /// 所有账户 (account_id -> account)
    accounts: HashMap<AccountId, Account>,
}

impl AccountManager {
    /// 创建账户管理器
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    /// 创建新账户
    pub fn create_account(&mut self, id: AccountId, initial_balance: f64) -> Result<(), String> {
        if self.accounts.contains_key(&id) {
            return Err(format!("Account {} already exists", id));
        }
        self.accounts.insert(id, Account::new(id, initial_balance));
        Ok(())
    }

    /// 获取账户（可变）
    pub fn get_account_mut(&mut self, id: AccountId) -> Result<&mut Account, String> {
        self.accounts
            .get_mut(&id)
            .ok_or_else(|| format!("Account {} not found", id))
    }

    /// 获取账户（只读）
    pub fn get_account(&self, id: AccountId) -> Result<&Account, String> {
        self.accounts
            .get(&id)
            .ok_or_else(|| format!("Account {} not found", id))
    }

    /// 验证买单资金
    pub fn validate_buy_order(
        &self,
        account_id: AccountId,
        symbol: &str,
        price: f64,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account(account_id)?;
        let required_balance = price * quantity;

        if account.available_balance() < required_balance {
            return Err(format!(
                "Insufficient balance: have {}, need {} for {} {} @ {}",
                account.available_balance(),
                required_balance,
                quantity,
                symbol,
                price
            ));
        }
        Ok(())
    }

    /// 验证卖单持仓
    pub fn validate_sell_order(
        &self,
        account_id: AccountId,
        symbol: &str,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account(account_id)?;
        let position = account
            .get_position(symbol)
            .ok_or_else(|| format!("No position in {}", symbol))?;

        if position.available() < quantity {
            return Err(format!(
                "Insufficient position: have {}, need {} of {}",
                position.available(),
                quantity,
                symbol
            ));
        }
        Ok(())
    }

    /// 冻结买单资金
    pub fn freeze_buy_order(
        &mut self,
        account_id: AccountId,
        price: f64,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account_mut(account_id)?;
        let required_balance = price * quantity;

        if account.available_balance() < required_balance {
            return Err("Insufficient balance to freeze".to_string());
        }

        account.frozen_balance += required_balance;
        Ok(())
    }

    /// 冻结卖单持仓
    pub fn freeze_sell_order(
        &mut self,
        account_id: AccountId,
        symbol: &str,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account_mut(account_id)?;
        let position = account.get_position_mut(symbol);

        if !position.freeze(quantity) {
            return Err("Insufficient position to freeze".to_string());
        }
        Ok(())
    }

    /// 执行买单成交（扣钱加仓位）
    pub fn execute_buy(
        &mut self,
        account_id: AccountId,
        symbol: &str,
        price: f64,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account_mut(account_id)?;
        let cost = price * quantity;

        // 更新资金
        if account.frozen_balance < cost {
            return Err("Frozen balance not enough".to_string());
        }
        account.frozen_balance -= cost;
        account.balance -= cost;

        // 更新持仓
        account.get_position_mut(symbol).execute_buy(quantity);
        Ok(())
    }

    /// 执行卖单成交（扣仓位加钱）
    pub fn execute_sell(
        &mut self,
        account_id: AccountId,
        symbol: &str,
        price: f64,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account_mut(account_id)?;
        let proceeds = price * quantity;

        // 更新持仓
        let position = account.get_position_mut(symbol);
        if !position.execute_sell(quantity) {
            return Err("Position not frozen or insufficient".to_string());
        }

        // 更新资金
        account.balance += proceeds;
        Ok(())
    }

    /// 取消订单（释放冻结资金/持仓）
    pub fn cancel_buy_order(
        &mut self,
        account_id: AccountId,
        price: f64,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account_mut(account_id)?;
        let cost = price * quantity;
        account.frozen_balance = (account.frozen_balance - cost).max(0.0);
        Ok(())
    }

    /// 取消卖单（释放冻结持仓）
    pub fn cancel_sell_order(
        &mut self,
        account_id: AccountId,
        symbol: &str,
        quantity: f64,
    ) -> Result<(), String> {
        let account = self.get_account_mut(account_id)?;
        let position = account.get_position_mut(symbol);
        position.unfreeze(quantity);
        Ok(())
    }

    /// 查询所有账户
    pub fn all_accounts(&self) -> Vec<&Account> {
        self.accounts.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_operations() {
        let mut pos = Position {
            quantity: 100.0,
            frozen: 0.0,
        };

        assert_eq!(pos.available(), 100.0);
        assert!(pos.freeze(30.0));
        assert_eq!(pos.available(), 70.0);
        assert_eq!(pos.frozen, 30.0);

        pos.unfreeze(20.0);
        assert_eq!(pos.frozen, 10.0);
        assert_eq!(pos.available(), 90.0);
    }

    #[test]
    fn test_account_balance() {
        let account = Account::new(1, 10000.0);
        assert_eq!(account.available_balance(), 10000.0);
    }

    #[test]
    fn test_account_manager() {
        let mut manager = AccountManager::new();

        // 创建账户
        assert!(manager.create_account(1, 50000.0).is_ok());
        assert!(manager.create_account(1, 10000.0).is_err()); // 重复创建应失败

        // 验证买单资金
        assert!(manager.validate_buy_order(1, "BTC", 100.0, 100.0).is_ok()); // 需要10000
        assert!(manager.validate_buy_order(1, "BTC", 100.0, 600.0).is_err()); // 需要60000，不够

        // 冻结资金
        assert!(manager.freeze_buy_order(1, 100.0, 100.0).is_ok());
        let acc = manager.get_account(1).unwrap();
        assert_eq!(acc.frozen_balance, 10000.0);
        assert_eq!(acc.available_balance(), 40000.0);
    }

    #[test]
    fn test_sell_position_validation() {
        let mut manager = AccountManager::new();
        manager.create_account(1, 100000.0).unwrap();

        // 先增加持仓
        {
            let account = manager.get_account_mut(1).unwrap();
            account.get_position_mut("BTC").quantity = 10.0;
        }

        // 验证卖单持仓
        assert!(manager.validate_sell_order(1, "BTC", 5.0).is_ok());
        assert!(manager.validate_sell_order(1, "BTC", 10.0).is_ok());
        assert!(manager.validate_sell_order(1, "BTC", 11.0).is_err());

        // 冻结持仓
        assert!(manager.freeze_sell_order(1, "BTC", 5.0).is_ok());
        let acc = manager.get_account(1).unwrap();
        let pos = acc.get_position("BTC").unwrap();
        assert_eq!(pos.frozen, 5.0);
        assert_eq!(pos.available(), 5.0);
    }
}
