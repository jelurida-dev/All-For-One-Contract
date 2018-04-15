package nxt.addons.contracts;

import nxt.Nxt;
import nxt.NxtException;
import nxt.addons.AbstractContractContext;
import nxt.addons.BlockContext;
import nxt.addons.RequestContext;
import nxt.blockchain.Chain;
import nxt.util.Convert;
import nxt.util.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class AllForOnePayment extends AbstractContract {

    /**
     * Contract triggered every several blocks, sums the received transactions and sends all of the amount back to one of the payers at random
     * Block based
     *
     * Contract Parameters
     * chain - chain to monitor
     * frequency - how many blocks to wait between payments
     * @param context contract context
     */
    @Override
    public void processBlock(BlockContext context) {
        // Read contract configuration
        JO contractConfigJson = context.getContractConfigJson(this.getClass().getCanonicalName());
        if (contractConfigJson == null) {
            context.setErrorResponse(10001, "contract configuration not specified");
            return;
        }
        int chainId = contractConfigJson.getInt("chain");
        int frequency = contractConfigJson.getInt("frequency");

        // Check if the it to perform payment distribution on this height
        JO block = context.getBlockJson();
        int height = block.getInt("height");
        if (height % frequency != 0) {
            context.setErrorResponse(10002,"%s: ignore block at height " + height, getClass().getName());
            return;
        }

        // Find the incoming payment transactions and calculate the payment amount
        String account = context.getConfig().getString("account");
        List<JO> payments = getPaymentTransactions(context, chainId, Math.max(height - frequency, 2), account);
        if (payments.size() == 0) {
            return;
        }
        long payment = payments.stream().mapToLong(t -> t.getLong("amountNQT")).sum();

        // Select random recipient account, your chance of being selected is proportional to the sum of your payments
        Random r = context.getRandom();
        DistributedRandomNumberGenerator distribution = new DistributedRandomNumberGenerator(r);
        payments.forEach(t -> distribution.addAccount(t.getLong("sender"), t.getLong("amountNQT")));
        long recipient = distribution.getRandomAccount();
        Logger.logInfoMessage(String.format("paying amount %d to account %s", payment, Convert.rsAccount(recipient)));

        // Calculate transaction fee and submit the payment transaction
        JO input = new JO();
        input.put("recipient", Long.toUnsignedString(recipient));
        input.put("amountNQT", payment);
        long feeNQT = context.getTransactionFee("sendMoney", Chain.getChain(chainId), input);
        if (feeNQT > 0) {
            input.put("feeNQT", feeNQT);
            input.put("amountNQT", payment - feeNQT);
        } else {
            context.setErrorResponse(10003,"%s: cannot calculate fee", getClass().getName());
            return;
        }
        context.createTransaction("sendMoney", Chain.getChain(chainId), input);
    }

    /**
     * Load all incoming payments to the contract account between the current height and the previously checked height
     * @param context contract context
     * @param chainId chain to monitor fot payments
     * @param height load transactions from this height until the current height
     * @param contractAccount the contract account
     * @return list of incoming payment transactions
     */
    private List<JO> getPaymentTransactions(AbstractContractContext context, int chainId, int height, String contractAccount) {
        // Get the block timestamp from which to load transactions and load the contract account transactions
        JO getBlockParams = new JO();
        getBlockParams.put("height", height);
        JO getBlockResponse = context.sendRequest("getBlock", getBlockParams);

        JO getBlockchainTransactionsParams = new JO();
        getBlockchainTransactionsParams.put("timestamp", getBlockResponse.get("timestamp"));
        getBlockchainTransactionsParams.put("account", context.getConfig().getString("accountRS"));
        getBlockchainTransactionsParams.put("executedOnly", "true");
        getBlockchainTransactionsParams.put("type", "" + (chainId == 1 ? (byte)-2 : (byte)0));
        getBlockchainTransactionsParams.put("subtype", "0");
        Logger.logInfoMessage("getPaymentTransactions params " + getBlockchainTransactionsParams.toJSONString());
        JO getBlockchainTransactionsResponse = context.sendRequest("getBlockchainTransactions", getBlockchainTransactionsParams);
        JA transactions = getBlockchainTransactionsResponse.getArray("transactions");

        // Filter the transactions by type and recipient, ignore transactions the contract sent to itself
        List<Object> temp = (List)transactions;
        return temp.stream().filter(t -> {
            JO transaction = (JO) t;
            Chain chain = Chain.getChain(transaction.getInt("chain"));
            byte type = transaction.getByte("type");
            byte subType = transaction.getByte("subtype");
            boolean isPayment = chain.getId() == chainId && (chain.getId() == 1 && type == -4 && subType == 0 || type == 0 && subType == 0);
            if (!isPayment) {
                return false;
            }
            if (!transaction.getString("recipient").equals(contractAccount)) {
                return false;
            }
            //noinspection RedundantIfStatement
            if (transaction.getString("recipient").equals(contractAccount) && transaction.getString("sender").equals(contractAccount)) {
                return false;
            }
            return true;
        }).map(t -> (JO)t).collect(Collectors.toList());
    }

    /**
     * Check the contract status - invoked by the invoke contract API before the distribution occurs.
     * Returns the existing payments made to the contract since the last payment.
     * @param context contract contract
     * @throws NxtException problem occurred
     */
    @Override
    public void processRequest(RequestContext context) throws NxtException {
        super.processRequest(context);
        JO response = context.getResponse();
        JO contractConfigJson = context.getContractConfigJson(this.getClass().getCanonicalName());
        if (contractConfigJson == null) {
            context.setErrorResponse(10001, "contract configuration not specified");
            return;
        }
        int chainId = contractConfigJson.getInt("chain");
        int frequency = contractConfigJson.getInt("frequency");
        String account = context.getConfig().getString("account");
        List<JO> payments = getPaymentTransactions(context, chainId, Math.max(Nxt.getBlockchain().getHeight() - frequency, 2), account);
        long payment = payments.stream().mapToLong(t -> t.getLong("amountNQT")).sum();
        response.put("paymentAmountNQT", payment);
        JA paymentsArray = new JA();
        for (JO paymentTransaction : payments) {
            JO paymentData = new JO();
            paymentData.put("senderRS", paymentTransaction.getString("senderRS"));
            paymentData.put("amountNQT", paymentTransaction.getLong("amountNQT"));
            paymentsArray.add(paymentData);
        }
        response.put("payments", paymentsArray);
        context.setResponse(response);
    }

    /**
     * Utility class to create weighted random numbers based on amount paid by each account
     */
    public static class DistributedRandomNumberGenerator {

        private final Map<Long, Double> distribution;
        private final Random random;
        private double sum;

        public DistributedRandomNumberGenerator(Random random) {
            this.random = random;
            this.distribution = new HashMap<>();
        }

        public void addAccount(long account, double amount) {
            if (distribution.get(account) != null) {
                amount += distribution.get(account);
                sum -= distribution.get(account);
            }
            distribution.put(account, amount);
            sum += amount;
        }

        public long getRandomAccount() {
            double ratio = 1.0f / sum;
            double runningSum = 0;
            for (long account : distribution.keySet()) {
                runningSum += distribution.get(account);
                if (random.nextDouble() / ratio <= runningSum) {
                    return account;
                }
            }
            return 0L; // Should never happen as there are registered accounts
        }
    }
}
