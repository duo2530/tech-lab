package dev.techplant.drools;

import dev.techplant.drools.model.Integral;
import dev.techplant.drools.model.Order;
import org.junit.jupiter.api.Test;
import org.kie.api.KieBase;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 每个 drl 文件对应一个测试方法，单独加载该文件并执行。
 * 在 IDEA 中点击方法名即可运行对应的 drl，无需修改参数或 DroolsConfig。
 */
class DrlRuleCases {

    /**
     * 单次会话允许的最大规则触发次数，超过即视为死循环
     */
    private static final int MAX_FIRES = 100;

    @Test
    void order_drl_金额30() {
        run("order.drl", 30);
    }

    @Test
    void salience_drl_金额30() {
        run("attributes_salience.drl", 30);
    }

    @Test
    void activationgroup_drl_金额30() {
        run("activationgroup.drl", 30);
    }
    @Test
    void activationgroup_drl_金额301() {
        run("activationgroup_loop.drl", 30);
    }

    @Test
    void attributes_enabled_drl() {
        run("attributes_enabled.drl", 30);
    }

    @Test
    void date_effective_drl() {
        run("attributes_effective.drl", 30);
    }

    @Test
    void attributes_expires_drl() {
        run("attributes_expires.drl", 30);
    }

    @Test
    void attributes_activation_group_drl() {
        run("attributes_activation_group.drl", 30);
    }

    @Test
    void attributes_agenda_group_drl() {
        run("attributes_agenda_group.drl", 30);
    }

    @Test
    void attributes_agenda_group_focus_drl() {
        KieHelper helper = new KieHelper();
        String fileName = "attributes_agenda_group_focus.drl";
        helper.addResource(ResourceFactory.newClassPathResource("rules/" + fileName), ResourceType.DRL);
        KieBase kieBase = helper.build();

        KieSession session = kieBase.newKieSession();
        try {
            Order order = new Order();
            order.setAmout(30);
            session.insert(order);

            //设置议程组焦点，组内规则才会执行
            session.getAgenda().getAgendaGroup("order_a").setFocus();

            int fired = session.fireAllRules(MAX_FIRES);

            System.out.println("文件: " + fileName
                    + "，金额: " + order.getAmout()
                    + "，积分: " + order.getScore()
                    + "，触发次数: " + fired);
            assertTrue(fired < MAX_FIRES, fileName + " 触发 " + fired + " 次，疑似死循环");
            assertTrue(order.getScore() == 10, "设置焦点后规则应执行，实际积分: " + order.getScore());
        } finally {
            session.dispose();
        }
    }

    @Test
    void attributes_timer_drl() throws InterruptedException {
        KieHelper helper = new KieHelper();
        String fileName = "attributes_timer.drl";
        helper.addResource(ResourceFactory.newClassPathResource("rules/" + fileName), ResourceType.DRL);
        KieBase kieBase = helper.build();

        KieSession session = kieBase.newKieSession();
        try {
            Order order = new Order();
            order.setAmout(30);
            session.insert(order);

            //定时器规则需要会话持续运行，在后台线程中 fireUntilHalt
            Thread worker = new Thread(session::fireUntilHalt);
            worker.start();

            //等待超过 1 秒的定时间隔，让定时器到期
            Thread.sleep(5000);
            session.halt();
            worker.join();

            System.out.println("文件: " + fileName
                    + "，金额: " + order.getAmout()
                    + "，积分: " + order.getScore());
            assertTrue(order.getScore() == 10, "定时器到期后规则应执行，实际积分: " + order.getScore());
        } finally {
            session.dispose();
        }
    }

    @Test
    void global_drl() {
        KieHelper helper = new KieHelper();
        String fileName = "global.drl";
        helper.addResource(ResourceFactory.newClassPathResource("rules/" + fileName), ResourceType.DRL);
        KieBase kieBase = helper.build();

        KieSession session = kieBase.newKieSession();
        try {
            Order order = new Order();
            order.setAmout(30);

            //全局变量
            Integral integral = new Integral();
            session.setGlobal("integral", integral);
            
            session.insert(order);

            int fired = session.fireAllRules(MAX_FIRES);

            System.out.println("文件: " + fileName
                    + "，金额: " + order.getAmout()
                    + "，积分: " + integral.getScore()
                    + "，触发次数: " + fired);
            assertTrue(fired < MAX_FIRES, fileName + " 触发 " + fired + " 次，疑似死循环");
        } finally {
            session.dispose();
        }
    }

    private void run(String fileName, double amount) {
        KieHelper helper = new KieHelper();
        helper.addResource(ResourceFactory.newClassPathResource("rules/" + fileName), ResourceType.DRL);
        KieBase kieBase = helper.build();

        KieSession session = kieBase.newKieSession();
        try {
            Order order = new Order();
            order.setAmout(amount);
            session.insert(order);

            int fired = session.fireAllRules(MAX_FIRES);

            System.out.println("文件: " + fileName
                    + "，金额: " + order.getAmout()
                    + "，积分: " + order.getScore()
                    + "，触发次数: " + fired);
            assertTrue(fired < MAX_FIRES, fileName + " 触发 " + fired + " 次，疑似死循环");
        } finally {
            session.dispose();
        }
    }
}
