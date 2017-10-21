package hbase.data; /**
 * Created by xiaoyue26 on 17/10/21.
 */

import java.util.Map;

/**
 * @author lism
 */
public class QuestionStat implements Cloneable {

    private int questionId;
    private int answerCount;
    private int correctCount;
    private long lastAnswerTime;
    private Map<Integer, Integer> optionAnswerCount;
    private double totalScore;
    private double totalElapsedTime;               // 总作答用时，单位：秒
    private int version;
    private int originVersion;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOriginVersion() {
        return originVersion;
    }

    public void setOriginVersion(int originVersion) {
        this.originVersion = originVersion;
    }

    public int getQuestionId() {
        return questionId;

    }

    public void setQuestionId(int questionId) {
        this.questionId = questionId;
    }

    public int getAnswerCount() {
        return answerCount;
    }

    public void setAnswerCount(int answerCount) {
        this.answerCount = answerCount;
    }

    public int getCorrectCount() {
        return correctCount;
    }

    public void setCorrectCount(int correctCount) {
        this.correctCount = correctCount;
    }

    public long getLastAnswerTime() {
        return lastAnswerTime;
    }

    public void setLastAnswerTime(long lastAnswerTime) {
        this.lastAnswerTime = lastAnswerTime;
    }

    public Map<Integer, Integer> getOptionAnswerCount() {
        return optionAnswerCount;
    }

    public void setOptionAnswerCount(Map<Integer, Integer> optionAnswerCount) {
        this.optionAnswerCount = optionAnswerCount;
    }

    public double getTotalScore() {
        return totalScore;
    }

    public void setTotalScore(double totalScore) {
        this.totalScore = totalScore;
    }

    public double getTotalElapsedTime() {
        return totalElapsedTime;
    }

    public void setTotalElapsedTime(double totalElapsedTime) {
        this.totalElapsedTime = totalElapsedTime;
    }

    @Override
    public String toString() {
        return "QuestionStat{" +
                "questionId=" + questionId +
                ", answerCount=" + answerCount +
                ", correctCount=" + correctCount +
                ", lastAnswerTime=" + lastAnswerTime +
                ", optionAnswerCount=" + optionAnswerCount +
                ", totalScore=" + totalScore +
                ", version=" + version +
                ", originVersion=" + originVersion +
                '}';
    }

}
