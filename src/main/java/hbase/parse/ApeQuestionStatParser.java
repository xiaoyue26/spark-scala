package hbase.parse;


import com.fenbi.hbase.util.KeyUtils;
import com.fenbi.hbase.util.RowMapperUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import hbase.data.QuestionStat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by wangs on 2017/6/23.
 */
public class ApeQuestionStatParser {

    private static final byte[] FAMILY = Bytes.toBytes("s");
    private static final byte[] COLUMN_ANSWER_COUNT = Bytes.toBytes("a");
    private static final byte[] COLUMN_CORRECT_COUNT = Bytes.toBytes("c");
    private static final byte[] COLUMN_LAST_ANSWER_TIME = Bytes.toBytes("t");
    private static final byte[] COLUMN_OPTION_ANSWER_COUNT = Bytes.toBytes("o");
    private static final byte[] COLUMN_TOTAL_SCORE = Bytes.toBytes("ts");
    private static final byte[] COLUMN_TOTAL_ELAPSED_TIME = Bytes.toBytes("tet");
    private static final byte[] COLUMN_VERSION = Bytes.toBytes("v");

    // reverse first key, random region
    private static final boolean REVERSE_QUESTION_ID = true;

    public static List<String> parse(Result result) throws Exception {
        QuestionStat stat = mapRow(result);
        List<String> res = new ArrayList<>();
        if (stat != null) {
            String common = String.format("%d\t%d\t%d\t%d\t%f\t%f\t%d\t%d",
                    stat.getQuestionId(), stat.getAnswerCount(), stat.getCorrectCount(), stat.getLastAnswerTime(),
                    stat.getTotalScore(), stat.getTotalElapsedTime(), stat.getVersion(), stat.getOriginVersion());
            Map<Integer, Integer> optionAnswerCount = stat.getOptionAnswerCount();
            if (MapUtils.isEmpty(optionAnswerCount)) {
                res.add(common);
            } else {
                optionAnswerCount.forEach((k,v)->{
                    String str = String.format("\t%d\t%d", k, v);
                    res.add(common + str);
                });
            }
        }
        return res;
    }

    private static QuestionStat mapRow(Result result) throws Exception {
        QuestionStat questionStat = new QuestionStat();

        questionStat.setQuestionId(KeyUtils.parseHBaseKey(result, REVERSE_QUESTION_ID));
        questionStat.setAnswerCount(RowMapperUtils.getInt(result, FAMILY, COLUMN_ANSWER_COUNT));
        questionStat.setCorrectCount(RowMapperUtils.getInt(result, FAMILY, COLUMN_CORRECT_COUNT));
        questionStat.setLastAnswerTime(RowMapperUtils.getLong(result, FAMILY, COLUMN_LAST_ANSWER_TIME));
        if (result.containsColumn(FAMILY, COLUMN_TOTAL_SCORE)) {
            questionStat.setTotalScore(RowMapperUtils.getDouble(result, FAMILY, COLUMN_TOTAL_SCORE));
        }
        if (result.containsColumn(FAMILY, COLUMN_TOTAL_ELAPSED_TIME)) {
            questionStat.setTotalElapsedTime(RowMapperUtils.getDouble(result, FAMILY, COLUMN_TOTAL_ELAPSED_TIME));
        }

        Map<byte[], byte[]> familyMap = result.getFamilyMap(FAMILY);

        questionStat.setOptionAnswerCount(
                familyMap.entrySet().stream()
                        .filter(
                                entry -> Bytes.toString(entry.getKey(), 0, 1).equals("o")
                        )
                        .collect(Collectors.toMap(
                                entry -> Bytes.toInt(entry.getKey(), 1),
                                entry -> Bytes.toInt(entry.getValue())
                        ))
        );

        questionStat.setVersion(RowMapperUtils.getInt(result, FAMILY, COLUMN_VERSION));

        return questionStat;
    }
}
