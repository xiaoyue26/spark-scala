package hbase.data;

/**
 * Created by xiaoyue26 on 17/10/23.
 */
public class TestUserQuestionStat {
    public static void main(String[] args) {
        UserQuestionStat uqs = new UserQuestionStat();
        uqs.setUserId(1);
        uqs.setQuestionId(2);
        uqs.setAnswerCount(1);
        uqs.setCorrectCount(1);
        uqs.setLastAnswerTime(123);
        System.out.println(uqs);
    }
}
