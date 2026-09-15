const WebSocket = require('ws');

/**
 * Deriv Martingale Bot - Three Column Strategy
 * 1. First Prediction: Single contract trade
 * 2. Initial Column (Under): Continuous Under trades
 * 3. Recovery Column (Over): Martingale recovery with Over trades
 */

class DerivMartingaleBot {
  constructor(config) {
    this.apiToken = config.apiToken;
    this.appId = config.appId || '1089';
    this.symbol = config.symbol || 'R_100'; // Volatility 100
    this.initialStake = config.initialStake || 10;
    this.maxMartingaleLevel = config.maxMartingaleLevel || 5;
    this.duration = config.duration || 5;
    this.durationUnit = config.durationUnit || 't'; // t = ticks
    this.currency = config.currency || 'USD';

    // Trading columns state
    this.state = {
      firstPrediction: {
        status: 'ready', // ready, waiting, completed
        contractId: null,
        prediction: null,
        stake: this.initialStake,
        result: null,
      },
      initialColumn: {
        status: 'ready',
        contractId: null,
        direction: 'PUT', // Under
        stake: this.initialStake,
        result: null,
        totalTrades: 0,
      },
      recoveryColumn: {
        status: 'idle',
        contractId: null,
        direction: 'CALL', // Over
        currentLevel: 0,
        stake: this.initialStake,
        result: null,
        totalLosses: 0,
        recovered: false,
      },
      accountBalance: 0,
      lastPrice: 0,
    };

    this.ws = null;
    this.messageId = 0;
    this.proposalCache = {};
  }

  /**
   * Connect to Deriv WebSocket API
   */
  connect() {
    return new Promise((resolve, reject) => {
      const wsUrl = `wss://ws.deriv.com/websockets/v3?app_id=${this.appId}`;
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        console.log('✅ Connected to Deriv API');
        this.authorize();
        resolve();
      };

      this.ws.onmessage = (event) => this.handleMessage(JSON.parse(event.data));

      this.ws.onerror = (error) => {
        console.error('❌ WebSocket Error:', error);
        reject(error);
      };

      this.ws.onclose = () => {
        console.log('⚠️ Connection closed');
        setTimeout(() => this.connect(), 3000); // Reconnect after 3 seconds
      };
    });
  }

  /**
   * Authorize with API token
   */
  authorize() {
    const msg = {
      authorize: this.apiToken,
      req_id: ++this.messageId,
    };
    this.send(msg);
  }

  /**
   * Get account balance
   */
  getBalance() {
    const msg = {
      balance: 1,
      subscribe: 1,
      req_id: ++this.messageId,
    };
    this.send(msg);
  }

  /**
   * Subscribe to price ticks
   */
  subscribeTicks() {
    const msg = {
      ticks: this.symbol,
      subscribe: 1,
      req_id: ++this.messageId,
    };
    this.send(msg);
  }

  /**
   * Send WebSocket message
   */
  send(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  /**
   * Handle incoming messages
   */
  handleMessage(response) {
    if (response.error) {
      console.error('❌ API Error:', response.error.message);
      return;
    }

    const msgType = response.msg_type;

    if (msgType === 'authorize') {
      console.log('✅ Authorized successfully');
      this.getBalance();
      this.subscribeTicks();
      this.startBot();
    } else if (msgType === 'balance') {
      this.state.accountBalance = response.balance?.balance || 0;
      console.log(`💰 Account Balance: ${this.state.accountBalance} ${this.currency}`);
    } else if (msgType === 'tick') {
      this.state.lastPrice = response.tick.quote;
      this.processTick();
    } else if (msgType === 'proposal') {
      this.handleProposal(response);
    } else if (msgType === 'buy') {
      this.handleBuy(response);
    } else if (msgType === 'sell') {
      this.handleSell(response);
    } else if (msgType === 'proposal_open_contract') {
      this.handleContractStatus(response);
    }
  }

  /**
   * Start the bot - execute trades based on strategy
   */
  startBot() {
    console.log('\n🚀 Bot Started - Three Column Strategy\n');
    console.log('📊 Columns:');
    console.log('   1. First Prediction - Single contract');
    console.log('   2. Initial Column (Under) - Continuous Under trades');
    console.log('   3. Recovery Column (Over) - Martingale recovery\n');

    // Start with First Prediction if not completed
    if (this.state.firstPrediction.status === 'ready') {
      this.executeFirstPrediction();
    }
  }

  /**
   * Process each price tick
   */
  processTick() {
    // Check First Prediction contract status
    if (this.state.firstPrediction.status === 'waiting') {
      this.checkContractStatus(this.state.firstPrediction.contractId, 'firstPrediction');
    }

    // Check Initial Column contract status
    if (this.state.initialColumn.status === 'waiting') {
      this.checkContractStatus(this.state.initialColumn.contractId, 'initialColumn');
    }

    // Check Recovery Column contract status
    if (this.state.recoveryColumn.status === 'waiting') {
      this.checkContractStatus(this.state.recoveryColumn.contractId, 'recoveryColumn');
    }

    // Execute trades based on current state
    this.executeTradeLogic();
  }

  /**
   * Execute First Prediction - Single contract
   */
  executeFirstPrediction() {
    console.log('\n🎯 First Prediction Column: Executing single contract...');

    // Randomly choose direction for first prediction
    const prediction = Math.random() > 0.5 ? 'CALL' : 'PUT';
    this.state.firstPrediction.prediction = prediction;

    const proposalMsg = {
      proposal: 1,
      amount: this.state.firstPrediction.stake,
      basis: 'stake',
      contract_type: prediction,
      currency: this.currency,
      duration: this.duration,
      duration_unit: this.durationUnit,
      symbol: this.symbol,
      req_id: ++this.messageId,
    };

    console.log(`   Direction: ${prediction} (${prediction === 'CALL' ? 'Over' : 'Under'})`);
    console.log(`   Stake: ${this.state.firstPrediction.stake}`);

    this.send(proposalMsg);
    this.state.firstPrediction.status = 'processing';
  }

  /**
   * Execute trades based on column states
   */
  executeTradeLogic() {
    // If First Prediction is completed, start Initial Column
    if (this.state.firstPrediction.status === 'completed') {
      if (this.state.initialColumn.status === 'ready') {
        this.executeInitialColumn();
      }
    }
  }

  /**
   * Execute Initial Column - Under trades (continuous)
   */
  executeInitialColumn() {
    if (this.state.recoveryColumn.status !== 'idle') {
      // Wait for recovery to complete
      return;
    }

    console.log('\n📍 Initial Column (Under): Requesting proposal...');

    const proposalMsg = {
      proposal: 1,
      amount: this.state.initialColumn.stake,
      basis: 'stake',
      contract_type: 'PUT', // Under
      currency: this.currency,
      duration: this.duration,
      duration_unit: this.durationUnit,
      symbol: this.symbol,
      req_id: ++this.messageId,
    };

    console.log(`   Direction: PUT (Under)`);
    console.log(`   Stake: ${this.state.initialColumn.stake}`);
    console.log(`   Trade #${this.state.initialColumn.totalTrades + 1}`);

    this.send(proposalMsg);
    this.state.initialColumn.status = 'processing';
  }

  /**
   * Execute Recovery Column - Over trades (Martingale)
   */
  executeRecoveryColumn() {
    if (this.state.recoveryColumn.currentLevel >= this.maxMartingaleLevel) {
      console.log('\n⚠️ Recovery Column: Max martingale level reached. Resetting...');
      this.resetRecoveryColumn();
      return;
    }

    this.state.recoveryColumn.currentLevel++;
    this.state.recoveryColumn.stake = this.initialStake * Math.pow(2, this.state.recoveryColumn.currentLevel - 1);

    console.log(`\n💰 Recovery Column (Over): Martingale Level ${this.state.recoveryColumn.currentLevel}`);

    const proposalMsg = {
      proposal: 1,
      amount: this.state.recoveryColumn.stake,
      basis: 'stake',
      contract_type: 'CALL', // Over
      currency: this.currency,
      duration: this.duration,
      duration_unit: this.durationUnit,
      symbol: this.symbol,
      req_id: ++this.messageId,
    };

    console.log(`   Direction: CALL (Over)`);
    console.log(`   Stake: ${this.state.recoveryColumn.stake}`);
    console.log(`   Loss Recovery Attempt #${this.state.recoveryColumn.currentLevel}`);

    this.send(proposalMsg);
    this.state.recoveryColumn.status = 'processing';
  }

  /**
   * Handle proposal response
   */
  handleProposal(response) {
    if (response.proposal) {
      const proposal = response.proposal;
      const id = response.req_id;

      // Store proposal
      this.proposalCache[id] = proposal;

      // Auto-buy the proposal
      const buyMsg = {
        buy: proposal.id,
        price: proposal.ask_price,
        req_id: ++this.messageId,
      };

      this.send(buyMsg);
    }
  }

  /**
   * Handle buy response
   */
  handleBuy(response) {
    if (response.buy) {
      const contract = response.buy;
      const contractId = contract.contract_id;

      if (this.state.firstPrediction.status === 'processing') {
        this.state.firstPrediction.contractId = contractId;
        this.state.firstPrediction.status = 'waiting';
        console.log(`   ✅ Bought - Contract ID: ${contractId}`);
      } else if (this.state.initialColumn.status === 'processing') {
        this.state.initialColumn.contractId = contractId;
        this.state.initialColumn.status = 'waiting';
        console.log(`   ✅ Bought - Contract ID: ${contractId}`);
      } else if (this.state.recoveryColumn.status === 'processing') {
        this.state.recoveryColumn.contractId = contractId;
        this.state.recoveryColumn.status = 'waiting';
        console.log(`   ✅ Bought - Contract ID: ${contractId}`);
      }
    }
  }

  /**
   * Check contract status
   */
  checkContractStatus(contractId, column) {
    const msg = {
      proposal_open_contract: 1,
      contract_id: contractId,
      subscribe: 0,
      req_id: ++this.messageId,
    };
    this.send(msg);
  }

  /**
   * Handle contract status
   */
  handleContractStatus(response) {
    const contract = response.proposal_open_contract;

    if (!contract) return;

    const contractId = contract.contract_id;
    const status = contract.status;
    const profit = contract.profit;
    const isWin = profit > 0;

    // Determine which column this contract belongs to
    let column = null;
    if (this.state.firstPrediction.contractId === contractId) {
      column = 'firstPrediction';
    } else if (this.state.initialColumn.contractId === contractId) {
      column = 'initialColumn';
    } else if (this.state.recoveryColumn.contractId === contractId) {
      column = 'recoveryColumn';
    }

    if (!column) return;

    // If contract is closed, process result
    if (status === 'closed') {
      this.processResult(column, isWin, profit);
    }
  }

  /**
   * Process trade result
   */
  processResult(column, isWin, profit) {
    const columnState = this.state[column];

    console.log(`\n${isWin ? '✅ WIN' : '❌ LOSS'} - ${column}`);
    console.log(`   Profit/Loss: ${profit}`);

    if (column === 'firstPrediction') {
      columnState.result = isWin ? 'win' : 'loss';
      columnState.status = 'completed';
      console.log('   First Prediction completed. Moving to Initial Column...');

      // Small delay before starting Initial Column
      setTimeout(() => {
        this.state.initialColumn.status = 'ready';
      }, 2000);
    } else if (column === 'initialColumn') {
      columnState.totalTrades++;

      if (isWin) {
        columnState.result = 'win';
        columnState.status = 'ready';
        console.log(`   Win! Total trades: ${columnState.totalTrades}`);

        // Reset recovery column if active
        if (this.state.recoveryColumn.status !== 'idle') {
          this.resetRecoveryColumn();
        }

        // Continue with next Under trade
        setTimeout(() => {
          this.state.initialColumn.status = 'ready';
        }, 1000);
      } else {
        columnState.result = 'loss';
        columnState.status = 'waiting';
        console.log(`   Loss! Triggering Recovery Column...`);

        // Trigger recovery column
        setTimeout(() => {
          this.state.recoveryColumn.status = 'ready';
          this.state.recoveryColumn.totalLosses++;
          this.executeRecoveryColumn();
        }, 1000);
      }
    } else if (column === 'recoveryColumn') {
      if (isWin) {
        columnState.result = 'win';
        columnState.recovered = true;
        console.log(`   ✅ Recovery Successful at Level ${columnState.currentLevel}!`);
        console.log(`   Total losses recovered: ${columnState.totalLosses}`);

        // Reset recovery and return to Initial Column
        this.resetRecoveryColumn();
        setTimeout(() => {
          this.state.initialColumn.status = 'ready';
        }, 1000);
      } else {
        columnState.result = 'loss';
        console.log(`   Level ${columnState.currentLevel} failed. Moving to next level...`);

        // Continue martingale sequence
        setTimeout(() => {
          this.state.recoveryColumn.status = 'ready';
          this.executeRecoveryColumn();
        }, 1000);
      }
    }
  }

  /**
   * Reset recovery column
   */
  resetRecoveryColumn() {
    this.state.recoveryColumn = {
      status: 'idle',
      contractId: null,
      direction: 'CALL',
      currentLevel: 0,
      stake: this.initialStake,
      result: null,
      totalLosses: this.state.recoveryColumn.totalLosses,
      recovered: false,
    };
    console.log('   Recovery Column reset.');
  }

  /**
   * Get bot stats
   */
  getStats() {
    return {
      accountBalance: this.state.accountBalance,
      firstPrediction: this.state.firstPrediction,
      initialColumn: this.state.initialColumn,
      recoveryColumn: this.state.recoveryColumn,
    };
  }

  /**
   * Stop the bot
   */
  stop() {
    if (this.ws) {
      this.ws.close();
      console.log('\n🛑 Bot stopped');
    }
  }
}

// Export for use
module.exports = DerivMartingaleBot;
