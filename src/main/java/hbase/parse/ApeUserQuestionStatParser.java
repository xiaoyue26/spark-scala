package hbase.parse;

import com.fenbi.common.data.IntPair;
import com.fenbi.hbase.util.KeyUtils;
import com.fenbi.hbase.util.RowMapperUtils;
import hbase.data.UserQuestionStat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoyue26 on 17/10/23.
 */
public class ApeUserQuestionStatParser {

    private static final byte[] FAMILY = Bytes.toBytes("d");
    private static final byte[] COLUMN_ANSWER_COUNT = Bytes.toBytes("a");
    private static final byte[] COLUMN_CORRECT_COUNT = Bytes.toBytes("c");
    private static final byte[] COLUMN_LAST_ANSWER_TIME = Bytes.toBytes("t");
    private static final boolean REVERSE_USER_ID = true;
    private static final boolean REVERSE_QUESTION_ID = false;

    public static List<String> parse(Result result) {
        /*List<String> res = new ArrayList<>();
        res.add("1\t2\t3\n");
        return res;*/
        UserQuestionStat uqs = mapRow(result);
        List<String> res = new ArrayList<>();
        if (uqs != null) {
            String common = String.format("%d\t%d\t%d\t%d\t%d",
                    uqs.getUserId()
                    , uqs.getQuestionId()
                    , uqs.getAnswerCount()
                    , uqs.getCorrectCount()
                    , uqs.getLastAnswerTime()
            );
            res.add(common);
        }
        return res;


    }

    public static UserQuestionStat mapRow(Result result) {
        if (result == null) {
            return null;
        }
        try {

            UserQuestionStat uqs = new UserQuestionStat();

            IntPair ids = KeyUtils.parseHBaseKey(result, REVERSE_USER_ID, REVERSE_QUESTION_ID);

            uqs.setUserId(ids.getV1());
            uqs.setQuestionId(ids.getV2());
            uqs.setAnswerCount(RowMapperUtils.getInt(result, FAMILY, COLUMN_ANSWER_COUNT));
            uqs.setCorrectCount(RowMapperUtils.getInt(result, FAMILY, COLUMN_CORRECT_COUNT));
            uqs.setLastAnswerTime(RowMapperUtils.getLong(result, FAMILY, COLUMN_LAST_ANSWER_TIME));
            return uqs;
        } catch (Exception e) {
            return null;
        }

    }
}
