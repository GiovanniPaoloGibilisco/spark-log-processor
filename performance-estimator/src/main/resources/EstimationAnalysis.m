
function EstimationAnalysis(x,y,y_estimate,training_size)

%commonly used percentiles for the gaussian
%68,3% = P{ ? - 1,00 ? < X < ? + 1,00 ? }
%95,0% = P{ ? - 1,96 ? < X < ? + 1,96 ? }
%95,5% = P{ ? - 2,00 ? < X < ? + 2,00 ? }
%99,0% = P{ ? - 2,58 ? < X < ? + 2,58 ? }
%99,7% = P{ ? - 3,00 ? < X < ? + 3,00 ? }
sigma_multiplier = 1.96

% calculate some statistics and variables
error = y-y_estimate;
relative_error = error./y_estimate;

%build the confidence interval for the estimation
[y_estimate_sup, y_estimate_inf] = ConfidenceIntervals(y,y_estimate,sigma_multiplier);

%build the training set
x_train = x(1:round(size(x,2)*training_size));
y_train = y(1:round(size(x,2)*training_size));

%train a linear model and build confidence intervals
model1 = polyfit(x_train,y_train,1);
y1 = polyval(model1,x);
[y1_sup, y1_inf] = ConfidenceIntervals(y,y1,sigma_multiplier);

%train a quadratic model and build confidence intervals
model2 = polyfit(x_train,y_train,2);
y2 = polyval(model2,x);
[y2_sup, y2_inf] = ConfidenceIntervals(y,y2,sigma_multiplier);


figure('name','Estimation Inspection','numbertitle','off')
%plot the estimation
subplot(2,2,1) % first subplot
hold on
grid on
plot(x,y,'.-')
plot(x,y_estimate)
plot(x,y_estimate_sup)
plot(x,y_estimate_inf)
xlabel('GB')
ylabel('ms')
legend('Duration','Estimate','Location','northwest')

%plot the estimation
subplot(2,2,2) % second subplot
hold on
grid on
plot(x,y,'.-')
plot(x,y_estimate)
plot(x,y1)
plot(x,y2)
xlabel('GB')
ylabel('ms')
legend('Duration','Estimate','Linear','Quadratic','Location','northwest')

%plot the error
subplot(2,2,3) % third subplot
hold on
grid on
plot(x,y,'.-')
plot(x,error)
xlabel('GB')
ylabel('ms')
legend('Duration','Error','Location','northwest')

%plot the error
subplot(2,2,4) % fourth subplot
plot(x,100*relative_error)
grid on
xlabel('GB')
ylabel('%')
legend('Relative Error','Location','northwest')
print('Estimation','-dpdf')



figure('name','Comparison','numbertitle','off')
%plot the estimation
subplot(2,2,1) % first subplot
hold on
grid on
plot(x,y,'.-')
plot(x,y_estimate)
plot(x,y_estimate_sup)
plot(x,y_estimate_inf)
xlabel('GB')
ylabel('ms')
legend('Duration','Estimate','Location','northwest')

%plot the estimation
subplot(2,2,2) % second subplot
hold on
grid on
plot(x,y,'.-')
plot(x,y1)
plot(x,y1_sup)
plot(x,y1_inf)
xlabel('GB')
ylabel('ms')
legend('Linear Estimation','Location','northwest')

%plot the error
subplot(2,2,3) % third subplot
hold on
grid on
plot(x,y,'.-')
plot(x,y2)
plot(x,y2_sup)
plot(x,y2_inf)
xlabel('GB')
ylabel('ms')
legend('Quadratic Estimation','Location','northwest')

%plot the estimation
subplot(2,2,4) % second subplot
hold on
grid on
plot(x,y,'.-')
plot(x,y_estimate)
plot(x,y1)
plot(x,y2)
xlabel('GB')
ylabel('ms')
legend('Duration','Estimate','Linear','Quadratic','Location','northwest')
print('Comparison','-dpdf')

end
